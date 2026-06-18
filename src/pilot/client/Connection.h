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
  enum class Result { Pending, Open, Failed, Close };
  enum class State { Idle, Connecting, Open, Closing, Closed, Failed };

  Connection(std::shared_ptr<WSclient> endpoint, std::string_view uri, std::stop_token token);
  ~Connection();

  Connection(const Connection &) = delete;
  Connection(Connection &&) = delete;

  void on_open(WSclient *c, websocketpp::connection_hdl hdl);
  void on_fail(WSclient *c, websocketpp::connection_hdl hdl);
  void on_close(WSclient *c, websocketpp::connection_hdl hdl);
  void on_message(websocketpp::connection_hdl, WSclient::message_ptr msg);

  [[nodiscard]] websocketpp::connection_hdl get_hdl() const { return m_connection->get_handle(); }

  ErrorOr<std::string> Send(std::string_view message);

  class FailedConnectionException : public websocketpp::exception {
  public:
    explicit FailedConnectionException(const std::string &what_arg) : websocketpp::exception{what_arg} {};
  };

private:
  std::string m_uri{};
  std::shared_ptr<WSclient> m_endpoint;
  WSclient::connection_ptr m_connection;
  std::string m_error_reason;

  Result m_connection_result{Result::Pending};
  std::mutex m_sendMutex;

  std::mutex cv_m;
  std::condition_variable cv;
  std::stop_token m_stop_token;

  void Close();
  ErrorOr<void> Connect();

  State m_state{State::Idle};
  mutable std::mutex m_state_mutex;
  State state() const;
  void set_state(State new_state);

  enum class RequestState {
    Inactive,
    InFlight,
    Completed,
    Error,
  };

  struct MessageReply {
  public:
    explicit MessageReply() {}

    void Activate();
    void TryCompleteSuccess(std::string_view message);
    void TryCompleteError();
    void Complete();

    std::future<std::string> Future() { return m_promise.get_future(); }

  private:
    RequestState m_request_state = RequestState::Inactive;
    std::promise<std::string> m_promise;
    std::mutex m_promise_mutex;
  } m_message_reply;
};

} // namespace PMS::Pilot

#endif
