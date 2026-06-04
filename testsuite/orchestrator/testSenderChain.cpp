// c++ headers
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// external dependencies
#include <catch2/catch_test_macros.hpp>
#include <exec/static_thread_pool.hpp>
#include <nlohmann/json.hpp>
#include <stdexec/execution.hpp>

// our headers
#include "common/Job.h"
#include "orchestrator/IDirector.h"
#include "orchestrator/Server.h"

using json = nlohmann::json;
using namespace PMS;
using namespace PMS::Orchestrator;

namespace {

// ---------------------------------------------------------------------------
// MinimalMockDirector — all methods return safe defaults
// ---------------------------------------------------------------------------
class MinimalMockDirector : public IDirector {
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

// ---------------------------------------------------------------------------
// ThrowingDirector — CreateTask throws to exercise upon_error deep in chain
// ---------------------------------------------------------------------------
class ThrowingDirector : public MinimalMockDirector {
public:
  Async<ErrorOr<std::string>> CreateTask(const std::string &) override {
    throw std::runtime_error("director exploded");
    co_return ErrorOr<std::string>{}; // makes this a coroutine; unreachable
  }
};

// ---------------------------------------------------------------------------
// ServerProxy — exposes protected sender helpers for testing
// ---------------------------------------------------------------------------
class ServerProxy : public Server {
public:
  explicit ServerProxy(std::shared_ptr<IDirector> d) : Server(std::move(d)) {}
  using Server::MakePilotReplySender;
  using Server::MakeUserReplySender;
};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

static exec::static_thread_pool g_pool{2};

static std::string run_user_message(ServerProxy &server, std::string_view payload) {
  auto result =
      stdexec::sync_wait(stdexec::on(g_pool.get_scheduler(), server.MakeUserReplySender(std::string{payload})));
  REQUIRE(result.has_value());
  auto [r] = result.value();
  return r;
}

static std::string run_pilot_message(ServerProxy &server, std::string_view payload) {
  auto result =
      stdexec::sync_wait(stdexec::on(g_pool.get_scheduler(), server.MakePilotReplySender(std::string{payload})));
  REQUIRE(result.has_value());
  auto [r] = result.value();
  return r;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// User-side pipeline tests
// ---------------------------------------------------------------------------

SCENARIO("MakeUserReplySender: malformed JSON triggers upon_error", "[Server][SenderChain]") {
  ServerProxy server{std::make_shared<MinimalMockDirector>()};

  GIVEN("a payload that is not valid JSON") {
    WHEN("the sender is driven to completion") {
      auto reply = run_user_message(server, "not valid json {{{");
      THEN("the reply contains the upon_error error message") {
        REQUIRE(reply.find("Invalid message, please check") != std::string::npos);
        REQUIRE(reply.find("JSON parse error") != std::string::npos);
      }
    }
  }
}

SCENARIO("MakeUserReplySender: valid JSON missing the command field", "[Server][SenderChain]") {
  ServerProxy server{std::make_shared<MinimalMockDirector>()};

  GIVEN("valid JSON with no 'command' key") {
    WHEN("the sender is driven to completion") {
      auto reply = run_user_message(server, R"({"foo": "bar"})");
      THEN("the reply reports a missing command field") { REQUIRE(reply.find("missing") != std::string::npos); }
    }
  }
}

SCENARIO("MakeUserReplySender: valid liveness probe returns OK", "[Server][SenderChain]") {
  ServerProxy server{std::make_shared<MinimalMockDirector>()};

  GIVEN("a livenessProbe JSON payload") {
    WHEN("the sender is driven to completion") {
      auto reply = run_user_message(server, R"({"livenessProbe": true})");
      THEN("the reply is 'OK'") { REQUIRE(reply == "OK"); }
    }
  }
}

SCENARIO("MakeUserReplySender: unknown command returns InvalidCommand reply", "[Server][SenderChain]") {
  ServerProxy server{std::make_shared<MinimalMockDirector>()};

  GIVEN("a JSON payload with an unknown command") {
    WHEN("the sender is driven to completion") {
      auto reply = run_user_message(server, R"({"command": "doSomethingUnknown"})");
      THEN("the reply contains 'not supported'") { REQUIRE(reply.find("not supported") != std::string::npos); }
    }
  }
}

SCENARIO("MakeUserReplySender: exception from HandleCommand is caught by upon_error", "[Server][SenderChain]") {
  ServerProxy server{std::make_shared<ThrowingDirector>()};

  GIVEN("a createTask command and a director that throws") {
    WHEN("the sender is driven to completion") {
      auto reply = run_user_message(server, R"({"command": "createTask", "task": "t1"})");
      THEN("the reply contains the error message from upon_error") {
        REQUIRE(reply.find("Invalid message, please check") != std::string::npos);
        REQUIRE(reply.find("director exploded") != std::string::npos);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Pilot-side pipeline tests
// ---------------------------------------------------------------------------

SCENARIO("MakePilotReplySender: malformed JSON triggers upon_error", "[Server][SenderChain]") {
  ServerProxy server{std::make_shared<MinimalMockDirector>()};

  GIVEN("a payload that is not valid JSON") {
    WHEN("the sender is driven to completion") {
      auto reply = run_pilot_message(server, "not valid json {{{");
      THEN("the reply contains the upon_error error message") {
        REQUIRE(reply.find("Invalid message, please check") != std::string::npos);
        REQUIRE(reply.find("JSON parse error") != std::string::npos);
      }
    }
  }
}

SCENARIO("MakePilotReplySender: valid p_test command returns ok", "[Server][SenderChain]") {
  ServerProxy server{std::make_shared<MinimalMockDirector>()};

  GIVEN("a p_test pilot command payload") {
    WHEN("the sender is driven to completion") {
      auto reply = run_pilot_message(server, R"({"command": "p_test"})");
      THEN("the reply is 'ok'") { REQUIRE(reply == "ok"); }
    }
  }
}

SCENARIO("MakePilotReplySender: unknown command returns InvalidCommand reply", "[Server][SenderChain]") {
  ServerProxy server{std::make_shared<MinimalMockDirector>()};

  GIVEN("a pilot JSON payload with an unknown command") {
    WHEN("the sender is driven to completion") {
      auto reply = run_pilot_message(server, R"({"command": "p_nonexistent"})");
      THEN("the reply contains 'not supported'") { REQUIRE(reply.find("not supported") != std::string::npos); }
    }
  }
}
