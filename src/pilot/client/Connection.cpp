#include <functional>
#include <utility>

#include <fmt/format.h>
#include <magic_enum.hpp>
#include <spdlog/spdlog.h>

#include "pilot/client/Connection.h"

namespace PMS::Pilot {

void Connection::Connect() {
  std::error_code ec;
  m_connection = m_endpoint->get_connection(std::string{m_uri}, ec);

  m_connection->set_open_handler([this](auto &&PH1) { on_open(m_endpoint.get(), std::forward<decltype(PH1)>(PH1)); });
  m_connection->set_fail_handler([this](auto &&PH1) { on_fail(m_endpoint.get(), std::forward<decltype(PH1)>(PH1)); });
  m_connection->set_close_handler([this](auto &&PH1) { on_close(m_endpoint.get(), std::forward<decltype(PH1)>(PH1)); });
  m_connection->set_message_handler([this](auto &&PH1, auto &&PH2) {
    on_message(std::forward<decltype(PH1)>(PH1), std::forward<decltype(PH2)>(PH2));
  });

#ifdef DEBUG_WEBSOCKETS
  m_endpoint->set_access_channels(websocketpp::log::alevel::all);
  m_endpoint->clear_access_channels(websocketpp::log::alevel::frame_payload);
  m_endpoint->set_error_channels(websocketpp::log::alevel::all);
#endif

  // manually increase open and close timeout
  m_endpoint->set_open_handshake_timeout(60000l);
  m_endpoint->set_close_handshake_timeout(60000l);

  spdlog::debug("Connecting...");
  m_endpoint->connect(m_connection);
  std::unique_lock<std::mutex> lk(cv_m);
  cv.wait(lk);
}

void Connection::Reconnect() {
  m_endpoint->reset();
  Connect();
}

Connection::Connection(std::shared_ptr<WSclient> endpoint, std::string_view uri)
    : m_uri{uri}, m_endpoint{std::move(endpoint)}, m_connection{nullptr} {
  Connect();
}

Connection::~Connection() {
  if (get_status() == State::open) {
    m_endpoint->close(get_hdl(), websocketpp::close::status::normal, "");
    std::unique_lock<std::mutex> lk(cv_m);
    cv.wait(lk);
  }
}

Connection::Connection(Connection &&rhs) noexcept
    : m_endpoint{std::move(rhs.m_endpoint)}, m_connection{std::move(rhs.m_connection)} {}

Connection &Connection::operator=(Connection &&rhs) noexcept {
  m_connection = rhs.m_connection;
  m_endpoint = std::move(rhs.m_endpoint);

  rhs.m_connection = nullptr;

  return *this;
}

void Connection::on_open([[maybe_unused]] WSclient *c, [[maybe_unused]] websocketpp::connection_hdl hdl) {
  spdlog::info("Connection established");

  std::lock_guard<std::mutex> lk(cv_m);
  cv.notify_all();
}

void Connection::on_fail(WSclient *c, websocketpp::connection_hdl hdl) {
  spdlog::error("Connection failed: {}", m_error_reason);

  // set an exception in the message promise, in case we were waiting for a message
  m_in_flight_message.set_exception(
      std::make_exception_ptr(FailedConnectionException(fmt::format("Connection close while sending a message"))));

  {
    std::lock_guard<std::mutex> lk(cv_m);
    cv.notify_all();
  }

  WSclient::connection_ptr con = c->get_con_from_hdl(std::move(hdl));
  m_error_reason = con->get_ec().message();
}

void Connection::on_close(WSclient *c, websocketpp::connection_hdl hdl) {
  spdlog::warn("Connection closed");

  // set an exception in the message promise, in case we were waiting for a message
  try {
    m_in_flight_message.set_exception(
        std::make_exception_ptr(FailedConnectionException(fmt::format("Connection close while sending a message"))));
  } catch (const std::future_error &e) {
    // connection was closed while we were NOT waiting for a message. No error to be raised.
  }

  {
    std::lock_guard<std::mutex> lk(cv_m);
    cv.notify_all();
  }

  WSclient::connection_ptr con = c->get_con_from_hdl(std::move(hdl));
  spdlog::trace("close code: {} ({}), close reason: {}", con->get_remote_close_code(),
                websocketpp::close::status::get_string(con->get_remote_close_code()), con->get_remote_close_reason());
}

void Connection::on_message(websocketpp::connection_hdl, WSclient::message_ptr msg) {
#ifdef DEBUG_WEBSOCKETS
  spdlog::trace("Received message: {}", msg->get_payload());
#endif
  m_in_flight_message.set_value(msg->get_payload());
}

ErrorOr<std::string> Connection::Send(const std::string &message) {
  std::lock_guard<std::mutex> slk(m_sendMutex);
  spdlog::trace("Send - lock acquired");

  std::promise<std::string>{}.swap(m_in_flight_message);
  auto message_future = m_in_flight_message.get_future();

  if (get_status() == State::closed || get_status() == State::closing) {
    spdlog::warn("Re-connecting to server...");
    Reconnect();
  }

  std::error_code ec;
  m_endpoint->send(get_hdl(), message, websocketpp::frame::opcode::text, ec);

#ifdef DEBUG_WEBSOCKETS
  spdlog::trace("waiting for message...");
#endif

  spdlog::trace("Send - releasing lock...");
  try {
    return message_future.get();
  } catch (const FailedConnectionException &e) {
    return Error{std::make_error_code(std::errc::connection_reset), e.what()};
  } catch (const std::future_error &e) {
    return Error{std::make_error_code(std::errc::device_or_resource_busy), e.what()};
  } catch (const std::exception &e) {
    return Error{std::make_error_code(std::errc::io_error), e.what()};
  }
}

} // namespace PMS::Pilot
