// c++ headers
#include <chrono>

// external dependencies
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "orchestrator/Server.h"

// https://github.com/okdshin/PicoSHA2
#include "orchestrator/picosha2.h"

using json = nlohmann::json;

namespace PMS {
namespace Orchestrator {
Server::~Server() {
  if (m_isRunning) {
    Stop();
  }
}

void Server::message_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg) {
  spdlog::trace("Received message {}", msg->get_payload());

  json job;
  try {
    job = json::parse(msg->get_payload());
  } catch (const std::exception &e) {
    spdlog::error("Error in parsing message: {}", e.what());
    m_endpoint.send(hdl, "Invalid job description, please check... :|", websocketpp::frame::opcode::text);
    return;
  }

  spdlog::trace("Received a valid job :)");

  // create an hash for this job
  std::string job_hash;
  picosha2::hash256_hex_string(msg->get_payload(), job_hash);
  job["hash"] = job_hash;

  // TODO: insert job in backend DB

  m_endpoint.send(hdl, fmt::format("Job received, generated hash: {}", job_hash), websocketpp::frame::opcode::text);
}

void Server::Listen() {
  constexpr unsigned int maxTries = 10;

  // Listen on designated port
  for (unsigned int iTry = 0; iTry < maxTries; iTry++) {
    try {
      m_endpoint.listen(m_port);
      spdlog::debug("Port {} acquired.", m_port);
      return;
    } catch (const std::exception &e) {
      spdlog::debug("Error in acquiring port... retrying... {} / {}", iTry, maxTries);
      std::this_thread::sleep_for(std::chrono::seconds{10});
    }
  }

  spdlog::error("Impossible to acquire port {}.", m_port);
}

void Server::Start() {
  spdlog::info("Starting Websocket server");

  // Set logging settings
  m_endpoint.set_error_channels(websocketpp::log::elevel::all);
  m_endpoint.set_access_channels(websocketpp::log::alevel::none);

  // Initialize Asio
  m_endpoint.init_asio();

  // Set the default message handler to our own handler
  m_endpoint.set_message_handler(
      std::bind(&Server::message_handler, this, std::placeholders::_1, std::placeholders::_2));

  Listen();

  // Queues a connection accept operation
  m_endpoint.start_accept();
  m_isRunning = true;

  // Start the Asio io_service run loop
  m_endpoint.run();
}

void Server::Stop() {
  spdlog::info("Stopping Websocket server");
  m_endpoint.stop();
  m_endpoint.stop_listening();
  m_isRunning = false;
}
} // namespace Orchestrator
} // namespace PMS