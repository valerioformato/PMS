#include <utility>

#include <spdlog/spdlog.h>

#include "pilot/client/Client.h"

namespace PMS::Pilot {
Client::Client(std::string serverUri) : m_serverUri{std::move(serverUri)}, m_endpoint{std::make_shared<WSclient>()} {
  m_endpoint->clear_access_channels(websocketpp::log::alevel::all);
  m_endpoint->clear_error_channels(websocketpp::log::elevel::all);

  m_endpoint->init_asio();
  m_endpoint->start_perpetual();

  m_thread = std::thread(&WSclient::run, m_endpoint.get());
}

Client::~Client() {
  m_endpoint->stop_perpetual();

  m_thread.join();
}

std::unique_ptr<Connection> Client::PersistentConnection() { return PersistentConnection(m_serverUri); }
std::unique_ptr<Connection> Client::PersistentConnection(std::string_view uri) {
  auto conn_ptr = std::make_unique<Connection>(m_endpoint, uri);

  unsigned int nTries = 0;
  while (
      (conn_ptr->get_status() == Connection::State::closing || conn_ptr->get_status() == Connection::State::closed) &&
      ++nTries < nMaxTries) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    spdlog::warn("Retrying... {}/{}", nTries, nMaxTries);
    conn_ptr = std::make_unique<Connection>(m_endpoint, uri);
  }

  if ((conn_ptr->get_status() == Connection::State::closing || conn_ptr->get_status() == Connection::State::closed)) {
    spdlog::error("Could not establish a connection after {} tries. Aborting...", nMaxTries);
  }

  return conn_ptr;
}

} // namespace PMS::Pilot
