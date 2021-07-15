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

std::unordered_map<std::string, Server::UserCommand> Server::m_commandLUT{
    // user available commands
    {"submitJob", UserCommand::SubmitJob},
    {"createTask", UserCommand::CreateTask},
    {"cleanTask", UserCommand::CleanTask},
    {"declareTaskDependency", UserCommand::DeclareTaskDependency},
};

std::unordered_map<std::string, Server::PilotCommand> Server::m_pilot_commandLUT{
    // pilot available commands
    {"p_claimJob", PilotCommand::ClaimJob},
    {"p_updateJobStatus", PilotCommand::UpdateJobStatus},
    {"p_registerNewPilot", PilotCommand::RegisterNewPilot},
    {"p_updateHeartBeat", PilotCommand::UpdateHeartBeat},
    {"p_deleteHeartBeat", PilotCommand::DeleteHeartBeat},
};

std::pair<bool, std::string> Server::ValidateTaskToken(const json &msg) const {
  if (!msg.contains("token")) {
    return {false, "Message doesn't contain a token"};
  }

  bool tokenValid = msg.contains("filter") ? m_director->ValidateTaskToken(msg["filter"]["task"], msg["token"])
                                           : m_director->ValidateTaskToken(msg["task"], msg["token"]);

  if (!tokenValid) {
    return {false, fmt::format("Invalid token for task {}", msg["task"])};
  }

  return {true, {}};
}

std::string Server::HandleCommand(UserCommand command, const json &msg) {
  std::string reply;

  // TODO: add token validation where needed

  switch (command) {
  case UserCommand::SubmitJob: {
    // FIXME: use c++17 structured bindings when available
    auto dummy = ValidateTaskToken(msg);
    if (!dummy.first) {
      reply = dummy.second;
      break;
    }

    // create an hash for this job
    std::string job_hash;
    picosha2::hash256_hex_string(msg.dump(), job_hash);
    json job = msg["job"];
    job["hash"] = job_hash;
    job["task"] = msg["task"];

    auto result = m_director->AddNewJob(std::move(job));
    // note: don't use job after this line!

    reply = result == Director::OperationResult::Success ? fmt::format("Job received, generated hash: {}", job_hash)
                                                         : "Job submission failed.";
  } break;
  case UserCommand::CreateTask: {
    auto result_s = m_director->CreateTask(msg["task"]);
    reply = result_s.result == Director::OperationResult::Success
                ? fmt::format("Task {} created. Token: {}", msg["task"], result_s.token)
                : fmt::format("Failed to create task \"{}\"", msg["task"]);
  } break;
  case UserCommand::CleanTask: {
    // FIXME: use c++17 structured bindings when available
    auto dummy = ValidateTaskToken(msg);
    if (!dummy.first) {
      reply = dummy.second;
      break;
    }

    auto result = m_director->CleanTask(msg["task"]);
    reply = result == Director::OperationResult::Success ? fmt::format("Task \"{}\" cleaned", msg["task"])
                                                         : fmt::format("Failed to clean task \"{}\"", msg["task"]);
  } break;
  case UserCommand::DeclareTaskDependency: {
    // FIXME: use c++17 structured bindings when available
    auto dummy = ValidateTaskToken(msg);
    if (!dummy.first) {
      reply = dummy.second;
      break;
    }

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

std::string Server::HandleCommand(PilotCommand command, const json &msg) {
  std::string reply;

  // TODO: add token validation where needed

  switch (command) {
  case PilotCommand::ClaimJob: {
    // NB: token validation has already been done during pilot registration

    if (!msg.contains("pilotUuid")) {
      return R"(Invalid request. Missing "pilotUuid" key)";
    }

    reply = m_director->ClaimJob(msg).dump();
  } break;
  case PilotCommand::UpdateJobStatus: {
    // NB: token validation has already been done during pilot registration

    if (!msg.contains("pilotUuid") || !msg.contains("status") || !msg.contains("hash") || !msg.contains("task")) {
      return R"(Invalid request. Missing "pilotUuid", "status", "hash", or "task" key)";
    }

    auto result = m_director->UpdateJobStatus(msg);

    reply = result == Director::OperationResult::Success ? "Ok" : "Failed to change job status";
  } break;
  case PilotCommand::RegisterNewPilot: {
    auto result = m_director->RegisterNewPilot(msg);

    json replyDoc;
    replyDoc["validTasks"] = json::array({});
    for (const auto &task : result.validTasks) {
      replyDoc["validTasks"].push_back(task);
    }

    reply = result.result == Director::OperationResult::Success
                ? replyDoc.dump()
                : fmt::format("Could not register pilot {}", msg["pilotUuid"]);
  } break;
  case PilotCommand::UpdateHeartBeat: {
    auto result = m_director->UpdateHeartBeat(msg);

    reply = result == Director::OperationResult::Success ? "Ok" : "Failed to update heartbeat";
  } break;
  case PilotCommand::DeleteHeartBeat: {
    auto result = m_director->DeleteHeartBeat(msg);

    reply = result == Director::OperationResult::Success ? "Ok" : "Failed to delete heartbeat";

  } break;
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

void Server::pilot_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg) {
  m_logger->trace("Received pilot message {}", msg->get_payload());

  json parsedMessage;
  try {
    parsedMessage = json::parse(msg->get_payload());
  } catch (const std::exception &e) {
    m_logger->error("Error in parsing message: {}", e.what());
    m_pilot_endpoint.send(hdl, fmt::format("Invalid message, please check... :|\n  Error: {}", e.what()),
                          websocketpp::frame::opcode::text);
    return;
  }

  if (parsedMessage["command"].empty()) {
    m_logger->error("No command in message. Sending back error...");
    m_pilot_endpoint.send(hdl, "Invalid message, missing \"command\" field", websocketpp::frame::opcode::text);
    return;
  }

  m_logger->trace("Received a valid message :)");

  std::string reply = HandleCommand(m_pilot_commandLUT[parsedMessage["command"]], parsedMessage);

  m_pilot_endpoint.send(hdl, reply, websocketpp::frame::opcode::text);
}

void Server::SetupEndpoint(WSserver &endpoint, unsigned int port) {

  // Set logging settings
  endpoint.set_error_channels(websocketpp::log::elevel::all);
  endpoint.set_access_channels(websocketpp::log::alevel::none);

  // Initialize Asio
  endpoint.init_asio();

  constexpr unsigned int maxTries = 10;

  for (unsigned int iTry = 0; iTry < maxTries; iTry++) {
    try {
      // Listen on designated port
      endpoint.listen(port);
      m_logger->debug("Port {} acquired.", port);

      // Queues a connection accept operation
      endpoint.start_accept();
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

  // Set the default message handler to our own handler
  m_endpoint.set_message_handler(
      std::bind(&Server::message_handler, this, std::placeholders::_1, std::placeholders::_2));
  m_pilot_endpoint.set_message_handler(
      std::bind(&Server::pilot_handler, this, std::placeholders::_1, std::placeholders::_2));

  SetupEndpoint(m_endpoint, m_port);
  SetupEndpoint(m_pilot_endpoint, m_port + 1);

  m_isRunning = true;

  // Start the Asio io_service run loops
  std::thread t_endpoint{[this]() { m_endpoint.run(); }};
  std::thread t_pilot_endpoint{[this]() { m_pilot_endpoint.run(); }};

  t_endpoint.join();
  t_pilot_endpoint.join();
}

void Server::Stop() {
  m_logger->info("Stopping Websocket server");
  m_endpoint.stop();
  m_endpoint.stop_listening();
  m_pilot_endpoint.stop();
  m_pilot_endpoint.stop_listening();
  m_isRunning = false;
}
} // namespace Orchestrator
} // namespace PMS