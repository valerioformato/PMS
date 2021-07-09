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

  void Listen();

  void keepAliveUntilSignal(std::future<void> exitSignal);

  void message_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg);

  enum class Command { SubmitJob, CreateTask, CleanTask, DeclareTaskDependency };
  std::string HandleCommand(Command command, const json &msg);
  static std::unordered_map<std::string, Command> m_commandLUT;

  std::shared_ptr<spdlog::logger> m_logger;

  bool m_isRunning = false;
  unsigned int m_port;
  WSserver m_endpoint;
  std::shared_ptr<Director> m_director;
};

} // namespace Orchestrator
} // namespace PMS

#endif