// c++ headers
#include <chrono>
#include <vector>

// external dependencies
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "orchestrator/Server.h"

// https://github.com/okdshin/PicoSHA2
#include "orchestrator/picosha2.h"

using json = nlohmann::json;
using namespace std::string_view_literals;

namespace PMS::Orchestrator {
template <class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template <class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

Server::~Server() {
  if (m_isRunning) {
    Stop();
  }
}

std::unordered_map<std::string_view, Server::UserCommandType> Server::m_commandLUT{
    // user available commands
    {"submitJob"sv, UserCommandType::SubmitJob},
    {"createTask"sv, UserCommandType::CreateTask},
    {"cleanTask"sv, UserCommandType::CleanTask},
    {"declareTaskDependency"sv, UserCommandType::DeclareTaskDependency},
};

std::unordered_map<std::string_view, Server::PilotCommandType> Server::m_pilot_commandLUT{
    // pilot available commands
    {"p_claimJob"sv, PilotCommandType::ClaimJob},
    {"p_updateJobStatus"sv, PilotCommandType::UpdateJobStatus},
    {"p_registerNewPilot"sv, PilotCommandType::RegisterNewPilot},
    {"p_updateHeartBeat"sv, PilotCommandType::UpdateHeartBeat},
    {"p_deleteHeartBeat"sv, PilotCommandType::DeleteHeartBeat},
};

std::pair<bool, std::string> Server::ValidateTaskToken(std::string_view task, std::string_view token) const {
  auto tokenValid = m_director->ValidateTaskToken(task, token);

  if (!tokenValid) {
    return {false, fmt::format("Invalid token for task {}", task)};
  }

  return {true, {}};
}

std::string Server::HandleCommand(UserCommand &&command) {
  return std::visit(overloaded{
                        // Create a new task
                        [this](const OrchCommand<CreateTask> &ucmd) {
                          auto [result, token] = m_director->CreateTask(ucmd.cmd.task);
                          return result == Director::OperationResult::Success
                                     ? fmt::format("Task {} created. Token: {}", ucmd.cmd.task, token)
                                     : fmt::format("Failed to create task \"{}\"", ucmd.cmd.task);
                        },
                        // Remove an existing task
                        [this](const OrchCommand<CleanTask> &ucmd) {
                          auto [valid, serverReply] = ValidateTaskToken(ucmd.cmd.task, ucmd.cmd.token);
                          if (!valid) {
                            return serverReply;
                          }

                          auto result = m_director->CleanTask(ucmd.cmd.task);
                          return result == Director::OperationResult::Success
                                     ? fmt::format("Task \"{}\" cleaned", ucmd.cmd.task)
                                     : fmt::format("Failed to clean task \"{}\"", ucmd.cmd.task);
                        },
                        // Declare a dependency between tasks
                        [this](const OrchCommand<DeclareTaskDependency> &ucmd) {
                          auto [valid, serverReply] = ValidateTaskToken(ucmd.cmd.task, ucmd.cmd.token);
                          if (!valid) {
                            return serverReply;
                          }

                          auto result = m_director->AddTaskDependency(ucmd.cmd.task, ucmd.cmd.dependsOn);
                          return result == Director::OperationResult::Success
                                     ? fmt::format("Task \"{}\" now depends on task {}", ucmd.cmd.task,
                                                   ucmd.cmd.dependsOn)
                                     : fmt::format("Failed to add task dependency");
                        },
                        // Submit a new job
                        [this](OrchCommand<SubmitJob> &ucmd) {
                          auto [valid, serverReply] = ValidateTaskToken(ucmd.cmd.task, ucmd.cmd.token);
                          if (!valid) {
                            return serverReply;
                          }

                          // create an hash for this job
                          std::string job_hash;
                          ucmd.cmd.job["task"] = ucmd.cmd.task;
                          picosha2::hash256_hex_string(ucmd.cmd.job.dump(), job_hash);
                          ucmd.cmd.job["hash"] = job_hash;

                          auto result = m_director->AddNewJob(ucmd.cmd.job);

                          return result == Director::OperationResult::Success
                                     ? fmt::format("Job received, generated hash: {}", job_hash)
                                     : fmt::format("Job submission failed.");
                        },
                        // Handle errors
                        [](const OrchCommand<InvalidCommand> &ucmd) { return ucmd.cmd.errorMessage; },
                    },
                    command);
}

std::string Server::HandleCommand(PilotCommand &&command) {
  return std::visit(
      overloaded{
          [this](const OrchCommand<ClaimJob> &pcmd) { return m_director->ClaimJob(pcmd.cmd.uuid).dump(); },
          [this](const OrchCommand<UpdateJobStatus> &pcmd) {
            auto result = m_director->UpdateJobStatus(pcmd.cmd.uuid, pcmd.cmd.hash, pcmd.cmd.task, pcmd.cmd.status);
            return (result == Director::OperationResult::Success) ? fmt::format("Ok")
                                                                  : fmt::format("Failed to change job status");
          },
          [this](const OrchCommand<RegisterNewPilot> &pcmd) {
            const auto &[result, validTasks, invalidTasks] =
                m_director->RegisterNewPilot(pcmd.cmd.uuid, pcmd.cmd.user, pcmd.cmd.tasks);

            json replyDoc;
            replyDoc["validTasks"] = json::array({});
            for (const auto &task : validTasks) {
              replyDoc["validTasks"].push_back(task);
            }

            return (result == Director::OperationResult::Success)
                       ? replyDoc.dump()
                       : fmt::format("Could not register pilot {}", pcmd.cmd.uuid);
          },
          [this](const OrchCommand<UpdateHeartBeat> &pcmd) {
            auto result = m_director->UpdateHeartBeat(pcmd.cmd.uuid);

            return (result == Director::OperationResult::Success) ? fmt::format("Ok")
                                                                  : fmt::format("Failed to update heartbeat");
          },
          [this](const OrchCommand<DeleteHeartBeat> &pcmd) {
            auto result = m_director->DeleteHeartBeat(pcmd.cmd.uuid);

            return (result == Director::OperationResult::Success) ? fmt::format("Ok")
                                                                  : fmt::format("Failed to update heartbeat");
          },
          // Handle errors
          [](const OrchCommand<InvalidCommand> &pcmd) { return pcmd.cmd.errorMessage; },
      },
      command);
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

  if (!parsedMessage.contains("command")) {
    m_logger->error("No command in message. Sending back error...");
    m_endpoint.send(hdl, "Invalid message, missing \"command\" field", websocketpp::frame::opcode::text);
    return;
  }

  m_logger->trace("Received a valid message :)");

  std::string reply = HandleCommand(toUserCommand(parsedMessage));

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

  std::string reply = HandleCommand(toPilotCommand(parsedMessage));

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
  m_endpoint.set_message_handler([this](auto &&PH1, auto &&PH2) {
    message_handler(std::forward<decltype(PH1)>(PH1), std::forward<decltype(PH2)>(PH2));
  });
  m_pilot_endpoint.set_message_handler([this](auto &&PH1, auto &&PH2) {
    pilot_handler(std::forward<decltype(PH1)>(PH1), std::forward<decltype(PH2)>(PH2));
  });

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

UserCommand Server::toUserCommand(const json &msg) {
  auto command = msg["command"].get<std::string_view>();
  auto cmdTypeP = m_commandLUT.find(command);

  if (cmdTypeP == end(m_commandLUT))
    return OrchCommand<InvalidCommand>{fmt::format("Command {} not supported", command)};

  std::string errorMessage{};

  switch (cmdTypeP->second) {
  case UserCommandType::CreateTask:
    if (ValidateJsonCommand<CreateTask>(msg))
      return OrchCommand<CreateTask>{msg["task"]};

    // handle invalid fields:
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", CreateTask::requiredFields);
    break;
  case UserCommandType::CleanTask:
    if (ValidateJsonCommand<CleanTask>(msg))
      return OrchCommand<CleanTask>{msg["task"], msg["token"]};

    // handle invalid fields:
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", CleanTask::requiredFields);
    break;
  case UserCommandType::DeclareTaskDependency:
    if (ValidateJsonCommand<DeclareTaskDependency>(msg))
      return OrchCommand<DeclareTaskDependency>{msg["task"], msg["dependsOn"], msg["token"]};

    // handle invalid fields:
    errorMessage =
        fmt::format("Invalid command arguments. Required fields are: {}", DeclareTaskDependency::requiredFields);
    break;
  case UserCommandType::SubmitJob:
    if (ValidateJsonCommand<SubmitJob>(msg))
      return OrchCommand<SubmitJob>{msg["job"], msg["task"], msg["token"]};

    // handle invalid fields:
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", SubmitJob::requiredFields);
    break;
  }

  return OrchCommand<InvalidCommand>{errorMessage};
}

PilotCommand Server::toPilotCommand(const json &msg) {
  auto command = msg["command"].get<std::string_view>();
  auto cmdTypeP = m_pilot_commandLUT.find(command);

  if (cmdTypeP == end(m_pilot_commandLUT))
    return OrchCommand<InvalidCommand>{fmt::format("Command {} not supported", command)};

  std::string errorMessage{};

  switch (cmdTypeP->second) {
  case PilotCommandType::ClaimJob:
    if (ValidateJsonCommand<ClaimJob>(msg))
      return OrchCommand<ClaimJob>{msg["pilotUuid"]};

    // handle invalid fields:
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", ClaimJob::requiredFields);
    break;
  case PilotCommandType::UpdateJobStatus:
    if (ValidateJsonCommand<UpdateJobStatus>(msg) &&
        magic_enum::enum_cast<JobStatus>(msg["status"].get<std::string_view>()).has_value())
      return OrchCommand<UpdateJobStatus>{
          magic_enum::enum_cast<JobStatus>(msg["status"].get<std::string_view>()).value(), msg["pilotUuid"],
          msg["hash"], msg["task"]};

    // handle invalid fields:
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", UpdateJobStatus::requiredFields);
    break;
  case PilotCommandType::RegisterNewPilot:
    if (ValidateJsonCommand<RegisterNewPilot>(msg)) {
      std::vector<std::pair<std::string, std::string>> tasks;
      for (const auto &task : msg["tasks"]) {
        tasks.emplace_back(task["name"], task["token"]);
      }
      return OrchCommand<RegisterNewPilot>{msg["pilotUuid"], msg["user"], std::move(tasks)};
    }
    // handle invalid fields:
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", RegisterNewPilot::requiredFields);
    break;
  case PilotCommandType::UpdateHeartBeat:
    if (ValidateJsonCommand<UpdateHeartBeat>(msg))
      return OrchCommand<UpdateHeartBeat>{msg["uuid"]};

    // handle invalid fields:
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", UpdateHeartBeat::requiredFields);
    break;
  case PilotCommandType::DeleteHeartBeat:
    if (ValidateJsonCommand<DeleteHeartBeat>(msg))
      return OrchCommand<DeleteHeartBeat>{msg["uuid"]};

    // handle invalid fields:
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", DeleteHeartBeat::requiredFields);
    break;
  }

  return OrchCommand<InvalidCommand>{errorMessage};
}
} // namespace PMS::Orchestrator