#ifndef PMS_PILOT_CONNECTION_H
#define PMS_PILOT_CONNECTION_H

#include <websocketpp/client.hpp>
#include <websocketpp/config/asio_no_tls_client.hpp>

namespace PMS::Pilot {

using WSclient = websocketpp::client<websocketpp::config::asio_client>;

class Connection {
public:
  enum class State { Connecting, Open, Failed, Closed };

  Connection(std::shared_ptr<WSclient> endpoint, const std::string &uri);
  ~Connection();

  Connection(const Connection &) = delete;

  void on_open(WSclient *c, websocketpp::connection_hdl hdl);
  void on_fail(WSclient *c, websocketpp::connection_hdl hdl);
  void on_close(WSclient *c, websocketpp::connection_hdl hdl);
  void on_message(websocketpp::connection_hdl, WSclient::message_ptr msg);

  websocketpp::connection_hdl get_hdl() const { return m_connection->get_handle(); }

  State get_status() const { return m_status; }

  std::string Send(const std::string &message);

private:
  std::shared_ptr<WSclient> m_endpoint;
  WSclient::connection_ptr m_connection;
  State m_status;
  std::string m_server;
  std::string m_error_reason;
  std::promise<std::string> m_in_flight_message;

  std::mutex cv_m;
  std::condition_variable cv;
};

} // namespace PMS::Pilot

#endif