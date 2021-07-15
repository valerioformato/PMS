#ifndef PMS_PILOT_CLIENT_H
#define PMS_PILOT_CLIENT_H

// c++ headers
#include <map>
#include <memory>
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

namespace PMS {
namespace Pilot {

class Client {
public:
  Client(const std::string &serverUri);
  ~Client();

  std::unique_ptr<Connection> PersistentConnection();
  std::unique_ptr<Connection> PersistentConnection(const std::string &uri);

  std::string Send(const json &msg);
  std::string Send(const json &msg, const std::string &uri);

  // std::string Send(Connection &connection, const json &msg);

private:
  std::string m_serverUri;
  std::shared_ptr<WSclient> m_endpoint;

  std::thread m_thread;
  // std::vector<std::shared_ptr<Connection>> m_connList;
};

} // namespace Pilot
} // namespace PMS

#endif