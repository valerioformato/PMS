#include <functional>
#include <utility>

#include <fmt/format.h>
#include <magic_enum.hpp>
#include <spdlog/spdlog.h>

#include "pilot/client/Connection.h"

namespace PMS::Pilot {

Connection::Connection(std::shared_ptr<WSclient> endpoint, std::string_view uri)
    : m_status{State::Connecting}, m_endpoint{std::move(endpoint)}, m_connection{nullptr}, m_server("N/A") {
  std::error_code ec;
  m_connection = m_endpoint->get_connection(std::string{uri}, ec);

  if (ec)
    spdlog::error("{}", ec.message());

  m_connection->set_open_handler([this](auto &&PH1) { on_open(m_endpoint.get(), std::forward<decltype(PH1)>(PH1)); });
  m_connection->set_fail_handler([this](auto &&PH1) { on_fail(m_endpoint.get(), std::forward<decltype(PH1)>(PH1)); });
  m_connection->set_close_handler([this](auto &&PH1) { on_close(m_endpoint.get(), std::forward<decltype(PH1)>(PH1)); });
  m_connection->set_message_handler([this](auto &&PH1, auto &&PH2) {
    on_message(std::forward<decltype(PH1)>(PH1), std::forward<decltype(PH2)>(PH2));
  });

  m_endpoint->connect(m_connection);
  std::unique_lock<std::mutex> lk(cv_m);
  cv.wait(lk);
}

Connection::~Connection() {
  if (m_status == State::Open)
    m_endpoint->close(get_hdl(), websocketpp::close::status::normal, "");
}

void Connection::on_open(WSclient *c, websocketpp::connection_hdl hdl) {
  m_status = State::Open;

  std::lock_guard<std::mutex> lk(cv_m);
  cv.notify_all();

  WSclient::connection_ptr con = c->get_con_from_hdl(hdl);
  m_server = con->get_response_header("Server");
  spdlog::debug("Connection opened with server version {}", m_server);
}

void Connection::on_fail(WSclient *c, websocketpp::connection_hdl hdl) {
  m_status = State::Failed;

  std::lock_guard<std::mutex> lk(cv_m);
  cv.notify_all();

  WSclient::connection_ptr con = c->get_con_from_hdl(hdl);
  m_server = con->get_response_header("Server");
  m_error_reason = con->get_ec().message();
  spdlog::error("Connection failed: {}", m_error_reason);
}

void Connection::on_close(WSclient *c, websocketpp::connection_hdl hdl) {
  m_status = State::Closed;
  WSclient::connection_ptr con = c->get_con_from_hdl(hdl);

  spdlog::debug("close code: {} ({}), close reason: {}", con->get_remote_close_code(),
                websocketpp::close::status::get_string(con->get_remote_close_code()), con->get_remote_close_reason());
}

void Connection::on_message(websocketpp::connection_hdl, WSclient::message_ptr msg) {
  spdlog::trace("Message received: {}", msg->get_payload());
  m_in_flight_message.set_value(msg->get_payload());
}

std::string Connection::Send(const std::string &message) {
  std::promise<std::string>{}.swap(m_in_flight_message);

  std::error_code ec;
  m_endpoint->send(get_hdl(), message, websocketpp::frame::opcode::text, ec);

  if (ec) {
    switch (m_status) {
    case State::Failed:
      m_in_flight_message.set_exception(std::make_exception_ptr(FailedConnectionException{"Connection failed"}));
      break;
    default:
      m_in_flight_message.set_exception(
          std::make_exception_ptr(std::runtime_error(fmt::format("Error sending message: {}", ec.message()))));
      break;
    }
  }

  return m_in_flight_message.get_future().get();
}

} // namespace PMS::Pilot
