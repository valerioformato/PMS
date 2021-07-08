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

std::unordered_map<std::string, Server::Command> Server::m_commandLUT{
    {"submitJob", Command::SubmitJob},
    {"creteTask", Command::CreateTask},
    {"cleanTask", Command::CleanTask},
    {"declareTaskDependency", Command::DeclareTaskDependency},
};

std::string Server::HandleCommand(Command command, const json &msg) {
  std::string reply;

  // TODO: add token validation where needed

  switch (command) {
  case Command::SubmitJob: {
    // create an hash for this job
    std::string job_hash;
    picosha2::hash256_hex_string(msg.dump(), job_hash);
    json job = msg["job"];
    job["hash"] = job_hash;

    auto result = m_director->AddNewJob(std::move(job));
    // note: don't use job after this line!

    reply = result == Director::OperationResult::Success ? fmt::format("Job received, generated hash: {}", job_hash)
                                                         : "Job submission failed.";
  } break;
  case Command::CreateTask: {
    auto result_s = m_director->CreateTask(msg["task"]);
    reply = result_s.result == Director::OperationResult::Success
                ? result_s.token
                : fmt::format("Failed to create task \"{}\"", msg["task"]);
  } break;
  case Command::CleanTask: {
    auto result = m_director->CleanTask(msg["task"]);
    reply = result == Director::OperationResult::Success ? fmt::format("Task \"{}\" cleaned", msg["task"])
                                                         : fmt::format("Failed to clean task \"{}\"", msg["task"]);
  } break;
  case Command::DeclareTaskDependency: {
    auto result = m_director->AddTaskDependency(msg["task"], msg["dependsOn"]);
    reply = result == Director::OperationResult::Success
                ? fmt::format("Task \"{}\" now depends on task {}", msg["task"], msg["dependsOn"])
                : fmt::format("Failed to add task dependency");
  } break;
  default:
    reply = fmt::format("Command \"{}\" is not in the list of available commands", msg["command"]);
    break;
  }

  return reply;
}

void Server::message_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg) {
  m_logger->trace("Received message {}", msg->get_payload());

  json parsedMessage;
  try {
    parsedMessage = json::parse(msg->get_payload());
  } catch (const std::exception &e) {
    m_logger->error("Error in parsing message: {}", e.what());
    m_endpoint.send(hdl, fmt::format("Invalid message, please check... :|\n  Error: {}", e.what()),
                    websocketpp::frame::opcode::text);
    return;
  }

  if (parsedMessage["command"].empty()) {
    m_logger->error("No command in message. Sending back error...");
    m_endpoint.send(hdl, "Invalid message, missing \"command\" field", websocketpp::frame::opcode::text);
    return;
  }

  m_logger->trace("Received a valid message :)");

  std::string reply = HandleCommand(m_commandLUT[parsedMessage["command"]], parsedMessage);

  m_endpoint.send(hdl, reply, websocketpp::frame::opcode::text);
}

void Server::Listen() {
  constexpr unsigned int maxTries = 10;

  // Listen on designated port
  for (unsigned int iTry = 0; iTry < maxTries; iTry++) {
    try {
      m_endpoint.listen(m_port);
      m_logger->debug("Port {} acquired.", m_port);
      return;
    } catch (const std::exception &e) {
      m_logger->debug("Error in acquiring port... retrying... {} / {}", iTry, maxTries);
      std::this_thread::sleep_for(std::chrono::seconds{10});
    }
  }

  m_logger->error("Impossible to acquire port {}.", m_port);
}

void Server::Start() {
  m_logger->info("Starting Websocket server");

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
  m_logger->info("Stopping Websocket server");
  m_endpoint.stop();
  m_endpoint.stop_listening();
  m_isRunning = false;
}
} // namespace Orchestrator
} // namespace PMS