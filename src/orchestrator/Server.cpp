// c++ headers
#include <chrono>

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
                        [this](const UCommand<CreateTask> &ucmd) {
                          auto [result, token] = m_director->CreateTask(ucmd.cmd.task);
                          return result == Director::OperationResult::Success
                                     ? fmt::format("Task {} created. Token: {}", ucmd.cmd.task, token)
                                     : fmt::format("Failed to create task \"{}\"", ucmd.cmd.task);
                        },
                        // Remove an existing task
                        [this](const UCommand<CleanTask> &ucmd) {
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
                        [this](const UCommand<DeclareTaskDependency> &ucmd) {
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
                        [this](UCommand<SubmitJob> &ucmd) {
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
                                     : "Job submission failed.";
                        },
                        // Handle errors
                        [](const UCommand<InvalidCommand> &ucmd) { return ucmd.cmd.errorMessage; },
                    },
                    command);
}

// std::string Server::HandleCommand(UserCommandType command, const json &msg) {
//   std::string reply;
//
//   // TODO: add token validation where needed
//
//   switch (command) {
//   case UserCommandType::SubmitJob: {
//     auto [valid, serverReply] = ValidateTaskToken(msg);
//     if (!valid) {
//       return serverReply;
//     }
//
//     // create an hash for this job
//     std::string job_hash;
//     picosha2::hash256_hex_string(msg.dump(), job_hash);
//     json job = msg["job"];
//     job["hash"] = job_hash;
//     job["task"] = msg["task"];
//
//     auto result = m_director->AddNewJob(std::move(job));
//     // note: don't use job after this line!
//
//     reply = result == Director::OperationResult::Success ? fmt::format("Job received, generated hash: {}", job_hash)
//                                                          : "Job submission failed.";
//   } break;
//   case UserCommandType::CreateTask: {
//     auto [result, token] = m_director->CreateTask(msg["task"]);
//     reply = result == Director::OperationResult::Success ? fmt::format("Task {} created. Token: {}", msg["task"],
//     token)
//                                                          : fmt::format("Failed to create task \"{}\"", msg["task"]);
//   } break;
//   case UserCommandType::CleanTask: {
//     auto [valid, serverReply] = ValidateTaskToken(msg);
//     if (!valid) {
//       return serverReply;
//     }
//
//     auto result = m_director->CleanTask(msg["task"]);
//     reply = result == Director::OperationResult::Success ? fmt::format("Task \"{}\" cleaned", msg["task"])
//                                                          : fmt::format("Failed to clean task \"{}\"", msg["task"]);
//   } break;
//   case UserCommandType::DeclareTaskDependency: {
//     auto [valid, serverReply] = ValidateTaskToken(msg);
//     if (!valid) {
//       return serverReply;
//     }
//
//     auto result = m_director->AddTaskDependency(msg["task"], msg["dependsOn"]);
//     reply = result == Director::OperationResult::Success
//                 ? fmt::format("Task \"{}\" now depends on task {}", msg["task"], msg["dependsOn"])
//                 : fmt::format("Failed to add task dependency");
//   } break;
//   default:
//     reply = fmt::format("Command \"{}\" is not in the list of available commands", msg["command"]);
//     break;
//   }
//
//   return reply;
// }

std::string Server::HandleCommand(PilotCommandType command, const json &msg) {
  std::string reply;

  // NB: token validation has already been done during pilot registration
  switch (command) {
  case PilotCommandType::ClaimJob: {
    if (!msg.contains("pilotUuid")) {
      return R"(Invalid request. Missing "pilotUuid" key)";
    }

    reply = m_director->ClaimJob(msg).dump();
  } break;
  case PilotCommandType::UpdateJobStatus: {
    if (!msg.contains("pilotUuid") || !msg.contains("status") || !msg.contains("hash") || !msg.contains("task")) {
      return R"(Invalid request. Missing "pilotUuid", "status", "hash", or "task" key)";
    }

    auto result = m_director->UpdateJobStatus(msg);

    reply = result == Director::OperationResult::Success ? "Ok" : "Failed to change job status";
  } break;
  case PilotCommandType::RegisterNewPilot: {
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
  case PilotCommandType::UpdateHeartBeat: {
    auto result = m_director->UpdateHeartBeat(msg);

    reply = result == Director::OperationResult::Success ? "Ok" : "Failed to update heartbeat";
  } break;
  case PilotCommandType::DeleteHeartBeat: {
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

  //  std::string reply = HandleCommand(m_pilot_commandLUT[parsedMessage["command"]], parsedMessage);
  std::string reply{};

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
    return UCommand<InvalidCommand>{fmt::format("Command {} not supported", command)};

  std::string errorMessage{};

  switch (cmdTypeP->second) {
  case UserCommandType::CreateTask:
    if (ValidateJsonCommand<CreateTask>(msg))
      return UCommand<CreateTask>{msg["task"]};

    // handle invalid fields:
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", CreateTask::requiredFields);
    break;
  case UserCommandType::CleanTask:
    if (ValidateJsonCommand<CleanTask>(msg))
      return UCommand<CleanTask>{msg["task"], msg["token"]};
    break;
  case UserCommandType::DeclareTaskDependency:
    if (ValidateJsonCommand<DeclareTaskDependency>(msg))
      return UCommand<DeclareTaskDependency>{msg["task"], msg["dependsOn"], msg["token"]};
    break;
  case UserCommandType::SubmitJob:
    if (ValidateJsonCommand<SubmitJob>(msg))
      return UCommand<SubmitJob>{msg["job"], msg["task"], msg["token"]};
    break;
  };

  return UCommand<InvalidCommand>{errorMessage};
}
} // namespace PMS::Orchestrator