#ifndef PMS_PILOT_CONNECTION_H
#define PMS_PILOT_CONNECTION_H

// c++ headers
#include <string_view>

// external dependencies
#include <websocketpp/client.hpp>
#include <websocketpp/config/asio_no_tls_client.hpp>
#include <websocketpp/connection.hpp>

#include "common/Utils.h"

namespace PMS::Pilot {

using WSclient = websocketpp::client<websocketpp::config::asio_client>;

class Connection {
public:
  Connection(std::shared_ptr<WSclient> endpoint, std::string_view uri);
  ~Connection();

  Connection(const Connection &) = delete;
  Connection(Connection &&) noexcept;

  Connection &operator=(Connection &&) noexcept;

  void on_open(WSclient *c, websocketpp::connection_hdl hdl);
  void on_fail(WSclient *c, websocketpp::connection_hdl hdl);
  void on_close(WSclient *c, websocketpp::connection_hdl hdl);
  void on_message(websocketpp::connection_hdl, WSclient::message_ptr msg);

  [[nodiscard]] websocketpp::connection_hdl get_hdl() const { return m_connection->get_handle(); }

  using State = websocketpp::session::state::value;
  [[nodiscard]] State get_status() const { return m_connection->get_state(); }

  ErrorOr<std::string> Send(const std::string &message);

  class FailedConnectionException : public websocketpp::exception {
  public:
    explicit FailedConnectionException(const std::string &what_arg) : websocketpp::exception{what_arg} {};
  };

private:
  std::string m_uri{};
  std::shared_ptr<WSclient> m_endpoint;
  WSclient::connection_ptr m_connection;
  std::string m_error_reason;
  std::promise<std::string> m_in_flight_message;

  std::mutex m_sendMutex;

  std::mutex cv_m;
  std::condition_variable cv;

  void Connect();
  void Reconnect();
};

} // namespace PMS::Pilot

#endif
