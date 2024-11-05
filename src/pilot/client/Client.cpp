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

  m_thread.join();
}

// NOTE: This function is not used in the current implementation
//       It was meant to be used as a one-shot way to send a message
[[maybe_unused]] ErrorOr<std::string> Client::Send(const json &msg) { return TRY(Send(msg, m_serverUri)); }
[[maybe_unused]] ErrorOr<std::string> Client::Send(const json &msg, std::string_view uri) {
  Connection connection{m_endpoint, uri};

  unsigned int nTries = 0;
  while (
      (connection.get_status() == Connection::State::closing || connection.get_status() == Connection::State::closed) &&
      ++nTries < nMaxTries) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    spdlog::warn("Retrying... {}/{}", nTries, nMaxTries);
    connection = Connection(m_endpoint, uri);
  }

  if ((connection.get_status() == Connection::State::closing || connection.get_status() == Connection::State::closed)) {
    spdlog::error("Could not establish a connection after {} tries. Aborting...", nMaxTries);
  }

  return TRY(connection.Send(msg.dump()));
}

std::unique_ptr<Connection> Client::PersistentConnection() { return PersistentConnection(m_serverUri); }
std::unique_ptr<Connection> Client::PersistentConnection(std::string_view uri) {
  auto connPtr = std::make_unique<Connection>(m_endpoint, uri);

  unsigned int nTries = 0;
  while ((connPtr->get_status() == Connection::State::closing || connPtr->get_status() == Connection::State::closed) &&
         ++nTries < nMaxTries) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    spdlog::warn("Retrying... {}/{}", nTries, nMaxTries);
    connPtr = std::make_unique<Connection>(m_endpoint, uri);
  }

  if ((connPtr->get_status() == Connection::State::closing || connPtr->get_status() == Connection::State::closed)) {
    spdlog::error("Could not establish a connection after {} tries. Aborting...", nMaxTries);
  }

  return connPtr;
}

} // namespace PMS::Pilot
