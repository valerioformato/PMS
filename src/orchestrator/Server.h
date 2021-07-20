#ifndef PMS_ORCHESTRATOR_SERVER_H
#define PMS_ORCHESTRATOR_SERVER_H

// c++ headers
#include <future>
#include <memory>
#include <thread>

// external dependencies
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>

// our headers
#include "orchestrator/Director.h"
#include "orchestrator/Commands.h"

using WSserver = websocketpp::server<websocketpp::config::asio>;

namespace PMS::Orchestrator {
class Server {
public:
  Server(unsigned int port, std::shared_ptr<Director> director)
      : m_logger{spdlog::stdout_color_st("Server")}, m_port{port}, m_director{std::move(director)} {}
  ~Server();

  void Start();
  void Stop();

private:
  std::pair<bool, std::string> ValidateTaskToken(std::string_view task, std::string_view token) const;

  void SetupEndpoint(WSserver &endpoint, unsigned int port);

  void message_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg);
  void pilot_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg);

  enum class UserCommandType { SubmitJob, CreateTask, CleanTask, DeclareTaskDependency };
  std::string HandleCommand(UserCommand &&command);
  static UserCommand toUserCommand(const json& msg);
  static std::unordered_map<std::string_view, UserCommandType> m_commandLUT;

  enum class PilotCommandType { ClaimJob, UpdateJobStatus, RegisterNewPilot, UpdateHeartBeat, DeleteHeartBeat };
  std::string HandleCommand(PilotCommand &&command);
  static PilotCommand toPilotCommand(const json& msg);
  static std::unordered_map<std::string_view, PilotCommandType> m_pilot_commandLUT;

  std::shared_ptr<spdlog::logger> m_logger;

  bool m_isRunning = false;
  unsigned int m_port;
  WSserver m_endpoint;
  WSserver m_pilot_endpoint;
  std::shared_ptr<Director> m_director;
};

} // namespace PMS::Orchestrator

#endif