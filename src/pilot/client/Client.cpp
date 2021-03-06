#include <algorithm>
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

  // FIXME: cleanup open connections upon destruction?
  // FIXME: use structured bindings when we go to c++17
  // for (auto &connection : m_connList) {
  //   if (connection->get_status() != Connection::State::Open) {
  //     // Only close open connections
  //     continue;
  //   }

  //   std::error_code ec;
  //   m_endpoint->close(connection->get_hdl(), websocketpp::close::status::going_away, "", ec);
  //   if (ec) {
  //     spdlog::error("> Error closing connection: ", ec.message());
  //   }
  // }

  m_thread.join();
}

std::string Client::Send(const json &msg) { return Send(msg, m_serverUri); }
std::string Client::Send(const json &msg, std::string_view uri) {
  Connection connection{m_endpoint, uri};

  unsigned int nTries = 0;
  while (connection.get_status() == Connection::State::Failed && ++nTries < nMaxTries) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    spdlog::warn("Retrying... {}/{}", nTries, nMaxTries);
    connection = Connection(m_endpoint, uri);
  }

  if (connection.get_status() == Connection::State::Failed) {
    spdlog::error("Could not establish a connection after {} tries. Aborting...", nMaxTries);
  }


  return connection.Send(msg.dump());
}

std::unique_ptr<Connection> Client::PersistentConnection() { return PersistentConnection(m_serverUri); }
std::unique_ptr<Connection> Client::PersistentConnection(std::string_view uri) {
  auto connPtr = std::make_unique<Connection>(m_endpoint, uri);

  unsigned int nTries = 0;
  while (connPtr->get_status() == Connection::State::Failed && ++nTries < nMaxTries) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    spdlog::warn("Retrying... {}/{}", nTries, nMaxTries);
    connPtr = std::make_unique<Connection>(m_endpoint, uri);
  }

  if (connPtr->get_status() == Connection::State::Failed) {
    spdlog::error("Could not establish a connection after {} tries. Aborting...", nMaxTries);
  }

  return connPtr;
}

} // namespace PMS::Pilot