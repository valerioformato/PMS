// external dependencies
#include <spdlog/spdlog.h>

// our headers
#include "orchestrator/Server.h"

namespace PMS {
namespace Orchestrator {
Server::~Server() {
  spdlog::debug("Stopping Websocket server");
  m_exitSignal.set_value();
  m_thread.join();
}

void Server::keepAliveUntilSignal(std::future<void> exitSignal) { // check if we need to shutdown server
  while (exitSignal.wait_for(std::chrono::milliseconds(1)) == std::future_status::timeout) {
  }

  m_endpoint.stop_listening();
}

void Server::Start() {
  // Set logging settings
  m_endpoint.set_error_channels(websocketpp::log::elevel::all);
  m_endpoint.set_access_channels(websocketpp::log::alevel::all ^ websocketpp::log::alevel::frame_payload);

  // Initialize Asio
  m_endpoint.init_asio();

  // Set the default message handler to the echo handler
  m_endpoint.set_message_handler(std::bind(&Server::echo_handler, this, std::placeholders::_1, std::placeholders::_2));

  // Listen on port 9002
  m_endpoint.listen(9002);

  // Queues a connection accept operation
  m_endpoint.start_accept();

  // Start the Asio io_service run loop
  m_endpoint.run();
}
} // namespace Orchestrator
} // namespace PMS