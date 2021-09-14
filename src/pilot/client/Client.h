#ifndef PMS_PILOT_CLIENT_H
#define PMS_PILOT_CLIENT_H

// c++ headers
#include <map>
#include <memory>
#include <string_view>
#include <thread>
#include <utility>

// external headers
#include <nlohmann/json.hpp>
#include <websocketpp/client.hpp>
#include <websocketpp/config/asio_no_tls.hpp>

// our headers
#include "pilot/client/Connection.h"

using json = nlohmann::json;

using WSclient = websocketpp::client<websocketpp::config::asio>;

namespace PMS::Pilot {

class Client {
public:
  explicit Client(std::string serverUri);
  ~Client();

  std::unique_ptr<Connection> PersistentConnection();
  std::unique_ptr<Connection> PersistentConnection(std::string_view uri);

  std::string Send(const json &msg);
  std::string Send(const json &msg, std::string_view uri);

  // std::string Send(Connection &connection, const json &msg);

private:
  std::string m_serverUri;
  std::shared_ptr<WSclient> m_endpoint;

  std::thread m_thread;

  constexpr static unsigned int nMaxTries = 10;
};

} // namespace PMS::Pilot

#endif