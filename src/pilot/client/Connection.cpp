#include <functional>
#include <utility>

#include <magic_enum/magic_enum.hpp>
#include <spdlog/fmt/bundled/format.h>
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
  m_connection_result = Result::Pending;
  m_endpoint->connect(m_connection);
  std::unique_lock<std::mutex> lk(cv_m);
  bool connected =
      cv.wait_for(lk, std::chrono::seconds(60), [this]() { return m_connection_result != Result::Pending; });

  if (!connected) {
    throw FailedConnectionException("Connection failed");
  }
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
    std::error_code ec;
    m_endpoint->close(get_hdl(), websocketpp::close::status::normal, "", ec);
    if (ec) {
      spdlog::error("{}", ec.message());
      return;
    }
    std::unique_lock<std::mutex> lk(cv_m);
    cv.wait_for(lk, std::chrono::seconds(60),
                [this]() { return m_connection_result == Result::Close || m_connection_result == Result::Failed; });
  }
}

void Connection::on_open([[maybe_unused]] WSclient *c, [[maybe_unused]] websocketpp::connection_hdl hdl) {
  spdlog::info("Connection established");

  std::lock_guard<std::mutex> lk(cv_m);
  m_connection_result = Result::Open;
  cv.notify_all();
}

void Connection::on_fail(WSclient *c, websocketpp::connection_hdl hdl) {
  spdlog::error("Connection failed: {}", m_error_reason);

  m_message_reply.TryCompleteError();

  {
    std::lock_guard<std::mutex> lk(cv_m);
    m_connection_result = Result::Failed;
    cv.notify_all();
  }

  WSclient::connection_ptr con = c->get_con_from_hdl(std::move(hdl));
  m_error_reason = con->get_ec().message();
}

void Connection::on_close(WSclient *c, websocketpp::connection_hdl hdl) {
  spdlog::warn("Connection closed");

  m_message_reply.TryCompleteError();

  {
    std::lock_guard<std::mutex> lk(cv_m);
    m_connection_result = Result::Close;
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
  m_message_reply.TryCompleteSuccess(msg->get_payload());
}

ErrorOr<std::string> Connection::Send(std::string_view message) {
  std::lock_guard<std::mutex> slk(m_sendMutex);
#ifdef DEBUG_WEBSOCKETS
  spdlog::trace("Send - lock acquired");
#endif

  m_message_reply.Activate();
  auto message_future = m_message_reply.Future();

  if (get_status() == State::closed || get_status() == State::closing) {
    spdlog::warn("Re-connecting to server...");
    Reconnect();
  }

  std::error_code ec;

#ifdef DEBUG_WEBSOCKETS
  spdlog::trace("Sending message: {}", message);
#endif

  m_endpoint->send(get_hdl(), std::string{message}, websocketpp::frame::opcode::text, ec);
  if (ec) {
    return make_error(ec, ec.message());
  }

#ifdef DEBUG_WEBSOCKETS
  spdlog::trace("waiting for message...");

  spdlog::trace("Send - releasing lock...");
#endif

  try {
    m_message_reply.Complete();
    return message_future.get();
  } catch (const FailedConnectionException &e) {
    return make_error(std::make_error_code(std::errc::connection_reset), e.what());
  } catch (const std::future_error &e) {
    return make_error(std::make_error_code(std::errc::device_or_resource_busy), e.what());
  } catch (const std::exception &e) {
    return make_error(std::make_error_code(std::errc::io_error), e.what());
  }
}

void Connection::MessageReply::Activate() {
  std::lock_guard lock(m_promise_mutex);

  m_state = RequestState::InFlight;
  std::promise<std::string>{}.swap(m_promise);
}

void Connection::MessageReply::TryCompleteSuccess(std::string_view message) {
  std::lock_guard lock(m_promise_mutex);

  if (m_state != RequestState::InFlight) {
    return;
  }

  m_state = RequestState::Completed;
  m_promise.set_value(std::string{message});
}

void Connection::MessageReply::TryCompleteError() {
  std::lock_guard lock(m_promise_mutex);

  if (m_state != RequestState::InFlight) {
    return;
  }

  m_state = RequestState::Error;
  m_promise.set_exception(
      std::make_exception_ptr(FailedConnectionException(fmt::format("Connection close while sending a message"))));
}

void Connection::MessageReply::Complete() {
  std::lock_guard lock(m_promise_mutex);

  if (m_state != RequestState::InFlight) {
    return;
  }

  m_state = RequestState::Completed;
}

} // namespace PMS::Pilot
