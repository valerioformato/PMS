#ifndef PMS_PILOT_CLIENT_H
#define PMS_PILOT_CLIENT_H

// c++ headers
#include <map>
#include <memory>
#include <string_view>
#include <thread>
#include <utility>

// external headers
#include <websocketpp/client.hpp>
#include <websocketpp/config/asio_no_tls.hpp>

// our headers
#include "pilot/client/Connection.h"

using WSclient = websocketpp::client<websocketpp::config::asio>;

namespace PMS::Pilot {

class Client {
public:
  explicit Client(std::string serverUri);
  ~Client();

  std::unique_ptr<Connection> PersistentConnection();
  std::unique_ptr<Connection> PersistentConnection(std::string_view uri);

private:
  std::string m_serverUri;
  std::shared_ptr<WSclient> m_endpoint;

  std::thread m_thread;

  std::stop_source m_stop_source;

  constexpr static unsigned int nMaxTries = 10;
};

} // namespace PMS::Pilot

#endif
