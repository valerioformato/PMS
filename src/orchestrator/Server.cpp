// c++ headers
#include <chrono>
#include <ranges>
#include <thread>
#include <vector>

// external dependencies
#include <nlohmann/json.hpp>
#include <spdlog/fmt/bundled/format.h>
#include <spdlog/fmt/bundled/ostream.h>
#include <spdlog/fmt/bundled/ranges.h>
#include <spdlog/spdlog.h>

// our headers
#include "common/JsonUtils.h"
#include "common/Utils.h"
#include "orchestrator/CommandParser.h"
#include "orchestrator/Server.h"

// from https://github.com/okdshin/PicoSHA2
#include "orchestrator/picosha2.h"

using json = nlohmann::json;
using namespace std::string_view_literals;
using namespace PMS::JsonUtils;

namespace PMS::Orchestrator {
Server::~Server() {
  if (m_isRunning) {
    Stop();
  }
}

std::pair<bool, std::string> Server::ValidateTaskToken(std::string_view task, std::string_view token) const {
  auto queryResult = m_director->ValidateTaskToken(task, token);

  switch (queryResult) {
  case IDirector::OperationResult::Success:
    return {true, {}};
  case IDirector::OperationResult::ProcessError:
    return {false, fmt::format("Invalid token for task {}", task)};
  case IDirector::OperationResult::DatabaseError:
    return {false, fmt::format("Task {} does not exist", task)};
  }

  // dummy return
  return {false, {}};
}

PMS::Async<std::string> Server::HandleCommand(UserCommand &&command) const {
  co_return co_await std::visit(
      PMS::Utils::overloaded{
          // Liveness probe
          [this](const OrchCommand<LivenessProbe> &) -> PMS::Async<std::string> {
            m_logger->trace("Received liveness probe. Sending back OK...");
            co_return std::string{"OK"};
          },
          // Create a new task
          [this](const OrchCommand<CreateTask> &ucmd) -> PMS::Async<std::string> {
            auto result = co_await m_director->CreateTask(ucmd.cmd.task);
            co_return result ? fmt::format("Task {} created. Token: {}", ucmd.cmd.task, result.value())
                             : fmt::format("Failed to create task \"{}\"", ucmd.cmd.task);
          },
          // Remove an existing task
          [this](const OrchCommand<ClearTask> &ucmd) -> PMS::Async<std::string> {
            auto [valid, serverReply] = ValidateTaskToken(ucmd.cmd.task, ucmd.cmd.token);
            if (!valid) {
              co_return serverReply;
            }
            auto result = co_await m_director->ClearTask(ucmd.cmd.task);
            co_return result ? fmt::format("Task \"{}\" cleared", ucmd.cmd.task)
                             : fmt::format("Failed to clear task \"{}\"", ucmd.cmd.task);
          },
          // Remove jobs from an existing task
          [this](const OrchCommand<CleanTask> &ucmd) -> PMS::Async<std::string> {
            auto [valid, serverReply] = ValidateTaskToken(ucmd.cmd.task, ucmd.cmd.token);
            if (!valid) {
              co_return serverReply;
            }
            auto result = co_await m_director->ClearTask(ucmd.cmd.task, false);
            co_return result ? fmt::format("Task \"{}\" cleaned", ucmd.cmd.task)
                             : fmt::format("Failed to clean task \"{}\"", ucmd.cmd.task);
          },
          // Declare a dependency between tasks
          [this](const OrchCommand<DeclareTaskDependency> &ucmd) -> PMS::Async<std::string> {
            auto [valid, serverReply] = ValidateTaskToken(ucmd.cmd.task, ucmd.cmd.token);
            if (!valid) {
              co_return serverReply;
            }
            auto result = co_await m_director->AddTaskDependency(ucmd.cmd.task, ucmd.cmd.dependsOn);
            co_return result ? fmt::format(R"(Task "{}" now depends on task "{}")", ucmd.cmd.task, ucmd.cmd.dependsOn)
                             : fmt::format("Failed to add task dependency");
          },
          // Check if a task/token pair is valid
          [this](const OrchCommand<CheckTaskToken> &ucmd) -> PMS::Async<std::string> {
            auto [valid, serverReply] = ValidateTaskToken(ucmd.cmd.task, ucmd.cmd.token);
            if (!valid) {
              co_return serverReply;
            }
            co_return fmt::format("Task/token pair is valid.");
          },
          // Submit a new job
          [this](OrchCommand<SubmitJob> &ucmd) -> PMS::Async<std::string> {
            auto [valid, serverReply] = ValidateTaskToken(ucmd.cmd.task, ucmd.cmd.token);
            if (!valid) {
              co_return serverReply;
            }
            std::string job_hash;
            ucmd.cmd.job["task"] = ucmd.cmd.task;
            picosha2::hash256_hex_string(ucmd.cmd.job.dump(), job_hash);
            ucmd.cmd.job["hash"] = job_hash;
            auto result = m_director->AddNewJob(ucmd.cmd.job);
            co_return result == IDirector::OperationResult::Success
                ? fmt::format("Job received, generated hash: {}", job_hash)
                : fmt::format("Job submission failed.");
          },
          // query db for jobs info
          [this](const OrchCommand<FindJobs> &ucmd) -> PMS::Async<std::string> {
            auto result =
                co_await m_director->QueryBackDB(IDirector::QueryOperation::Find, ucmd.cmd.match, ucmd.cmd.filter);
            co_return result ? result.value() : std::string{result.error().Message()};
          },
          // reset jobs to pending status and 0 retries
          [this](const OrchCommand<ResetJobs> &ucmd) -> PMS::Async<std::string> {
            json updateAction;
            updateAction["$set"]["status"] = magic_enum::enum_name(JobStatus::Pending);
            updateAction["$set"]["retries"] = 0;
            auto result =
                co_await m_director->QueryBackDB(IDirector::QueryOperation::UpdateMany, ucmd.cmd.match, updateAction);
            co_return result ? result.value() : std::string{result.error().Message()};
          },
          // query db for pilot info
          [this](const OrchCommand<FindPilots> &ucmd) -> PMS::Async<std::string> {
            auto result =
                co_await m_director->QueryFrontDB(IDirector::DBCollection::Pilots, ucmd.cmd.match, ucmd.cmd.filter);
            co_return result ? result.value() : std::string{result.error().Message()};
          },
          // Get user summary
          [this](const OrchCommand<Summary> &ucmd) -> PMS::Async<std::string> {
            auto result = co_await m_director->Summary(ucmd.cmd.user);
            co_return result ? result.value() : std::string{result.error().Message()};
          },
          // Reset jobs in a given task that have status Failed
          [this](const OrchCommand<ResetFailedJobs> &ucmd) -> PMS::Async<std::string> {
            auto [valid, serverReply] = ValidateTaskToken(ucmd.cmd.task, ucmd.cmd.token);
            if (!valid) {
              co_return serverReply;
            }
            auto result = co_await m_director->ResetFailedJobs(ucmd.cmd.task);
            co_return result ? fmt::format("Jobs reset") : result.error().Message().data();
          },
          // Handle errors
          [this](const OrchCommand<InvalidCommand> &ucmd) -> PMS::Async<std::string> {
            m_logger->debug("Replying to invalid pilot command with {}", ucmd.cmd.errorMessage);
            co_return ucmd.cmd.errorMessage;
          },
      },
      command);
}

PMS::Async<std::string> Server::HandleCommand(PilotCommand &&command) const {
  co_return co_await std::visit(
      PMS::Utils::overloaded{// request a new job
                             [this](const OrchCommand<ClaimJob> &pcmd) -> PMS::Async<std::string> {
                               auto result = co_await m_director->PilotClaimJob(pcmd.cmd.uuid);
                               if (!result) {
                                 m_logger->error("{}", result.error().Message());
                               }
                               co_return result ? result.value().dump() : std::string{result.error().Message()};
                             },
                             // update job status
                             [this](const OrchCommand<UpdateJobStatus> &pcmd) -> PMS::Async<std::string> {
                               auto result = co_await m_director->UpdateJobStatus(pcmd.cmd.uuid, pcmd.cmd.hash,
                                                                                  pcmd.cmd.task, pcmd.cmd.status);
                               co_return result ? fmt::format("Ok") : std::string{result.error().Message()};
                             },
                             // register a new pilot
                             [this](const OrchCommand<RegisterNewPilot> &pcmd) -> PMS::Async<std::string> {
                               m_logger->trace("Registering new pilot: {}", pcmd.cmd.uuid);
                               const auto result = co_await m_director->RegisterNewPilot(
                                   pcmd.cmd.uuid, pcmd.cmd.user, pcmd.cmd.tasks, pcmd.cmd.tags, pcmd.cmd.host_info);
                               if (!result)
                                 co_return fmt::format("Could not register pilot {}", pcmd.cmd.uuid);

                               json replyDoc;
                               replyDoc["validTasks"] = json::array({});
                               for (const auto &task : result.value().validTasks) {
                                 replyDoc["validTasks"].push_back(task);
                               }

                               co_return replyDoc.dump();
                             },
                             // update pilot heartbeat
                             [this](const OrchCommand<UpdateHeartBeat> &pcmd) -> PMS::Async<std::string> {
                               auto result = m_director->UpdateHeartBeat(pcmd.cmd.uuid);
                               co_return result ? fmt::format("Ok") : fmt::format("Failed to update heartbeat");
                             },
                             // delete pilot
                             [this](const OrchCommand<DeleteHeartBeat> &pcmd) -> PMS::Async<std::string> {
                               auto result = co_await m_director->DeleteHeartBeat(pcmd.cmd.uuid);
                               co_return result ? fmt::format("Ok") : fmt::format("Failed to update heartbeat");
                             },
                             // Handle errors
                             [this](const OrchCommand<InvalidCommand> &pcmd) -> PMS::Async<std::string> {
                               m_logger->debug("Replying to invalid pilot command with {}", pcmd.cmd.errorMessage);
                               co_return pcmd.cmd.errorMessage;
                             },
                             // Stress tests
                             [this]([[maybe_unused]] const OrchCommand<Test> &pcmd) -> PMS::Async<std::string> {
                               co_return std::string{"ok"};
                             }},
      command);
}

PMS::Async<std::string> Server::MakeUserReplySender(std::string payload) {
  m_logger->trace("[{}] Received message {}", std::hash<std::thread::id>{}(std::this_thread::get_id()), payload);
  try {
    auto parsed = json::parse(payload, nullptr, false);
    if (parsed.is_discarded())
      throw std::runtime_error("JSON parse error");
    co_return co_await HandleCommand(CommandParser::toUserCommand(parsed));
  } catch (const std::exception &e) {
    m_logger->error("Error handling message: {}", e.what());
    co_return fmt::format("Invalid message, please check... :|\n  Error: {}", e.what());
  } catch (...) {
    m_logger->error("Unknown error handling message");
    co_return std::string{"Invalid message, please check... :|"};
  }
}

PMS::Async<std::string> Server::MakePilotReplySender(std::string payload) {
  m_logger->trace("[{}] Received pilot message {}", std::hash<std::thread::id>{}(std::this_thread::get_id()), payload);
  try {
    auto parsed = json::parse(payload, nullptr, false);
    if (parsed.is_discarded())
      throw std::runtime_error("JSON parse error");
    co_return co_await HandleCommand(CommandParser::toPilotCommand(parsed));
  } catch (const std::exception &e) {
    m_logger->error("Error handling pilot message: {}", e.what());
    co_return fmt::format("Invalid message, please check... :|\n  Error: {}", e.what());
  } catch (...) {
    m_logger->error("Unknown error handling pilot message");
    co_return std::string{"Invalid message, please check... :|"};
  }
}

void Server::message_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg) {
  auto payload = std::string{msg->get_payload()};

  exec::start_detached(
      stdexec::on(m_compute_pool.get_scheduler(),
                  MakeUserReplySender(std::move(payload)) | stdexec::then([this, hdl](std::string reply) {
                    m_logger->trace("Sending reply: {}", reply);
                    websocketpp::lib::error_code ec;
                    m_endpoint.send(hdl, reply, websocketpp::frame::opcode::text, ec);
                    if (ec)
                      m_logger->error("Error sending reply: {}", ec.message());
                  })));
}

void Server::pilot_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg) {
  auto payload = std::string{msg->get_payload()};

  exec::start_detached(
      stdexec::on(m_compute_pool.get_scheduler(),
                  MakePilotReplySender(std::move(payload)) | stdexec::then([this, hdl](std::string reply) {
                    m_logger->trace("Sending reply: {}", reply);
                    websocketpp::lib::error_code ec;
                    m_pilot_endpoint.send(hdl, reply, websocketpp::frame::opcode::text, ec);
                    if (ec)
                      m_logger->error("Error sending pilot reply: {}", ec.message());
                  })));
}

void Server::SetupEndpoint(WSserver &endpoint, unsigned int port) {

#ifdef DEBUG_WEBSOCKETS
  endpoint.set_error_channels(websocketpp::log::alevel::all);
  endpoint.set_access_channels(websocketpp::log::alevel::all);
  endpoint.clear_access_channels(websocketpp::log::alevel::frame_payload);
#else
  endpoint.set_error_channels(websocketpp::log::alevel::all);
  endpoint.set_access_channels(websocketpp::log::alevel::none);
#endif

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
      m_logger->error("Error in acquiring port {}... retrying... {} / {}", port, iTry, maxTries);
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

} // namespace PMS::Orchestrator
