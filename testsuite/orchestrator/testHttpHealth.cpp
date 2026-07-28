// c++ headers
#include <array>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// external dependencies
#include <boost/asio.hpp>
#include <catch2/catch_test_macros.hpp>
#include <nlohmann/json.hpp>

// our headers
#include "common/Job.h"
#include "orchestrator/IDirector.h"
#include "orchestrator/Server.h"

using json = nlohmann::json;
using namespace PMS;
using namespace PMS::Orchestrator;
using namespace std::chrono_literals;

namespace {

class NoopDirector : public IDirector {
public:
  OperationResult ValidateTaskToken(std::string_view, std::string_view) const override {
    return OperationResult::Success;
  }

  OperationResult AddNewJob(const json &) override { return OperationResult::Success; }
  OperationResult AddNewJob(json &&) override { return OperationResult::Success; }

  Async<ErrorOr<json>> PilotClaimJob(std::string_view) override { co_return ErrorOr<json>{json::object()}; }
  Async<ErrorOr<void>> UpdateJobStatus(std::string_view, std::string_view, std::string_view, JobStatus) override {
    co_return ErrorOr<void>{};
  }
  Async<ErrorOr<NewPilotResult>> RegisterNewPilot(std::string_view, std::string_view,
                                                  const std::vector<std::pair<std::string, std::string>> &,
                                                  const std::vector<std::string> &, const json &) override {
    co_return ErrorOr<NewPilotResult>{NewPilotResult{OperationResult::Success, {}, {}}};
  }
  ErrorOr<void> UpdateHeartBeat(std::string_view) override { return ErrorOr<void>{}; }
  Async<ErrorOr<void>> DeleteHeartBeat(std::string_view) override { co_return ErrorOr<void>{}; }
  Async<ErrorOr<void>> AddTaskDependency(const std::string &, const std::string &) override {
    co_return ErrorOr<void>{};
  }
  Async<ErrorOr<std::string>> CreateTask(const std::string &) override {
    co_return ErrorOr<std::string>{std::string{"mock-token"}};
  }
  Async<ErrorOr<void>> ClearTask(const std::string &, bool) override { co_return ErrorOr<void>{}; }
  Async<ErrorOr<std::string>> Summary(const std::string &) override {
    co_return ErrorOr<std::string>{std::string{"[]"}};
  }
  Async<ErrorOr<std::string>> QueryBackDB(QueryOperation, const json &, const json &) override {
    co_return ErrorOr<std::string>{std::string{R"({"result":[]})"}};
  }
  Async<ErrorOr<std::string>> QueryFrontDB(DBCollection, const json &, const json &) override {
    co_return ErrorOr<std::string>{std::string{R"({"result":[]})"}};
  }
  Async<ErrorOr<void>> ResetFailedJobs(std::string_view) override { co_return ErrorOr<void>{}; }
};

struct HttpResponse {
  unsigned int status;
  std::string body;
};

bool port_is_available(unsigned short port) {
  boost::asio::io_context context;
  boost::asio::ip::tcp::acceptor acceptor{context};
  boost::system::error_code error;
  acceptor.open(boost::asio::ip::tcp::v4(), error);
  if (error)
    return false;
  acceptor.bind({boost::asio::ip::tcp::v4(), port}, error);
  return !error;
}

unsigned short find_adjacent_ports() {
  for (unsigned int port = 30000; port < 60000; port += 2) {
    if (port_is_available(static_cast<unsigned short>(port)) &&
        port_is_available(static_cast<unsigned short>(port + 1))) {
      return static_cast<unsigned short>(port);
    }
  }
  throw std::runtime_error{"Could not find two adjacent ports for Server test"};
}

HttpResponse get(unsigned short port, std::string_view resource) {
  boost::asio::io_context context;
  boost::asio::ip::tcp::socket socket{context};
  socket.connect({boost::asio::ip::make_address("127.0.0.1"), port});

  const auto request = "GET " + std::string{resource} + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
  boost::asio::write(socket, boost::asio::buffer(request));

  boost::system::error_code error;
  std::string raw;
  for (std::array<char, 1024> buffer{};;) {
    const auto bytes = socket.read_some(boost::asio::buffer(buffer), error);
    raw.append(buffer.data(), bytes);
    if (error == boost::asio::error::eof)
      break;
    if (error)
      throw boost::system::system_error{error};
  }

  const auto status_start = raw.find(' ') + 1;
  const auto status_end = raw.find(' ', status_start);
  const auto body_start = raw.find("\r\n\r\n");
  if (status_start == std::string::npos || status_end == std::string::npos || body_start == std::string::npos)
    throw std::runtime_error{"Malformed HTTP response"};

  return {
      .status = static_cast<unsigned int>(std::stoul(raw.substr(status_start, status_end - status_start))),
      .body = raw.substr(body_start + 4),
  };
}

class RunningServer {
public:
  RunningServer()
      : m_port{find_adjacent_ports()}, m_server{m_port, std::make_shared<NoopDirector>(), 1}, m_thread{[this] {
          m_server.Start();
        }} {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
      try {
        static_cast<void>(::get(m_port, "/startup-check"));
        return;
      } catch (const boost::system::system_error &) {
        std::this_thread::sleep_for(10ms);
      }
    }

    m_server.Stop();
    m_thread.join();
    throw std::runtime_error{"Server did not start before test timeout"};
  }

  ~RunningServer() {
    m_server.Stop();
    if (m_thread.joinable())
      m_thread.join();
  }

  RunningServer(const RunningServer &) = delete;
  RunningServer &operator=(const RunningServer &) = delete;

  HttpResponse get(std::string_view resource) const { return ::get(m_port, resource); }

private:
  unsigned short m_port;
  Server m_server;
  std::thread m_thread;
};

} // namespace

SCENARIO("Server exposes an HTTP liveness endpoint", "[Server][HttpHealth]") {
  RunningServer server;

  WHEN("Kubernetes sends a plain HTTP request to /healthz") {
    const auto response = server.get("/healthz");

    THEN("the server reports that it is alive") {
      REQUIRE(response.status == 200);
      REQUIRE(response.body == "OK");
    }
  }
}

SCENARIO("Server rejects unknown plain HTTP resources", "[Server][HttpHealth]") {
  RunningServer server;

  WHEN("a plain HTTP request targets an unrelated resource") {
    const auto response = server.get("/not-a-health-endpoint");

    THEN("the resource is not reported as healthy") { REQUIRE(response.status == 404); }
  }
}
