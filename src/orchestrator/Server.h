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
  Server(unsigned int port, std::shared_ptr<Director> director) : m_port{port}, m_director{std::move(director)} {}
  ~Server();

  void Start();
  void Stop();

private:
  void Listen();

  void keepAliveUntilSignal(std::future<void> exitSignal);

  void echo_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg) {
    // write a new message
    m_endpoint.send(hdl, msg->get_payload(), msg->get_opcode());
  }

  void message_handler(websocketpp::connection_hdl hdl, WSserver::message_ptr msg);

  bool m_isRunning = false;
  unsigned int m_port;
  WSserver m_endpoint;
  std::shared_ptr<Director> m_director;
};

} // namespace Orchestrator
} // namespace PMS

#endif