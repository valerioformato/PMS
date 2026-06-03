#ifndef PMS_ORCHESTRATOR_SERVER_H
#define PMS_ORCHESTRATOR_SERVER_H

// c++ headers
#include <future>
#include <memory>
#include <string_view>
#include <thread>

// external dependencies
#include <exec/start_detached.hpp>
#include <exec/static_thread_pool.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>

// our headers
#include "orchestrator/Commands.h"
#include "orchestrator/IDirector.h"

using WSserver = websocketpp::server<websocketpp::config::asio>;

namespace PMS::Orchestrator {
class Server {
public:
  Server(unsigned int port, std::shared_ptr<IDirector> director, unsigned int connectionThreads)
      : m_logger{spdlog::get("Server") ? spdlog::get("Server") : spdlog::stdout_color_st("Server")}, m_port{port},
        m_director{std::move(director)}, m_thread_pool{connectionThreads} {}
  ~Server();

  void Start();
  void Stop();

protected:
  // Test injection constructor: skips WebSocket setup.
  explicit Server(std::shared_ptr<IDirector> director)
      : m_logger{spdlog::get("Server") ? spdlog::get("Server") : spdlog::stdout_color_st("Server")}, m_port{0},
        m_director{std::move(director)}, m_thread_pool{1} {}

  std::pair<bool, std::string> ValidateTaskToken(std::string_view task, std::string_view token) const;

  IDirector::Async<std::string> HandleCommand(UserCommand &&command) const;
  IDirector::Async<std::string> HandleCommand(PilotCommand &&command) const;

  IDirector::Async<std::string> MakeUserReplySender(std::string payload);
  IDirector::Async<std::string> MakePilotReplySender(std::string payload);

private:
  std::shared_ptr<spdlog::logger> m_logger;

  bool m_isRunning = false;
  unsigned int m_port;
  WSserver m_endpoint;
  WSserver m_pilot_endpoint;
  std::shared_ptr<IDirector> m_director;

  // m_thread_pool must be declared last so its destructor (which joins workers,
  // draining all in-flight start_detached chains) runs before any other member
  // is destroyed. This ensures detached chains can safely reference m_endpoint,
  // m_pilot_endpoint, m_director, and m_logger throughout their execution.
  exec::static_thread_pool m_thread_pool{32};

  void SetupEndpoint(WSserver &endpoint, unsigned int port);

  void message_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg);
  void pilot_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg);
};

} // namespace PMS::Orchestrator

#endif
