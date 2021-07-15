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

using WSserver = websocketpp::server<websocketpp::config::asio>;

namespace PMS {
namespace Orchestrator {
class Server {
public:
  Server(unsigned int port, std::shared_ptr<Director> director)
      : m_logger{spdlog::stdout_color_st("Server")}, m_port{port}, m_director{std::move(director)} {}
  ~Server();

  void Start();
  void Stop();

private:
  std::pair<bool, std::string> ValidateTaskToken(const json &msg) const;

  void SetupEndpoint(WSserver &endpoint, unsigned int port);

  void message_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg);
  void pilot_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg);

  enum class UserCommand { SubmitJob, CreateTask, CleanTask, DeclareTaskDependency };
  std::string HandleCommand(UserCommand command, const json &msg);
  static std::unordered_map<std::string, UserCommand> m_commandLUT;

  enum class PilotCommand { ClaimJob, UpdateJobStatus, RegisterNewPilot, UpdateHeartBeat, DeleteHeartBeat };
  std::string HandleCommand(PilotCommand command, const json &msg);
  static std::unordered_map<std::string, PilotCommand> m_pilot_commandLUT;

  std::shared_ptr<spdlog::logger> m_logger;

  bool m_isRunning = false;
  unsigned int m_port;
  WSserver m_endpoint;
  WSserver m_pilot_endpoint;
  std::shared_ptr<Director> m_director;
};

} // namespace Orchestrator
} // namespace PMS

#endif