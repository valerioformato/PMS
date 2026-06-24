#include <catch2/catch_test_macros.hpp>
#include <catch2/trompeloeil.hpp>

#include <atomic>
#include <memory>
#include <stop_token>
#include <thread>

#include "pilot/client/Connection.h"
#include <websocketpp/message_buffer/alloc.hpp>

namespace PMS::Pilot {

class ConnectionTestHelper {
public:
  static void SetState(Connection &conn, Connection::State new_state) { conn.set_state(new_state); }

  static Connection::State GetState(const Connection &conn) { return conn.state(); }

  static Connection::Result GetConnectionResult(const Connection &conn) { return conn.m_connection_result; }

  static void SetConnectionResult(Connection &conn, Connection::Result result) { conn.m_connection_result = result; }

  static std::string GetErrorReason(const Connection &conn) { return conn.m_error_reason; }

  static void SetErrorReason(Connection &conn, const std::string &reason) { conn.m_error_reason = reason; }

  static void SetConnection(Connection &conn, WSclient::connection_ptr con) { conn.m_connection = con; }

  static Connection::MessageReply &GetMessageReply(Connection &conn) { return conn.m_message_reply; }

  static const Connection::MessageReply &GetMessageReply(const Connection &conn) { return conn.m_message_reply; }

  using RequestState = Connection::RequestState;

  static bool IsStopRequested(const Connection &conn) { return conn.m_stop_token.stop_requested(); }

  static ErrorOr<void> Connect(Connection &conn) { return conn.Connect(); }

  static void Close(Connection &conn) { conn.Close(); }
};

} // namespace PMS::Pilot

namespace {

using namespace PMS::Pilot;

std::shared_ptr<WSclient> CreateMockEndpoint() {
  auto endpoint = std::make_shared<WSclient>();
  return endpoint;
}

} // namespace

SCENARIO("Connection: default state after no_connect construction", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a Connection constructed with no_connect") {
    THEN("state is Idle") { REQUIRE(ConnectionTestHelper::GetState(conn) == Connection::State::Idle); }
    THEN("connection result is Pending") {
      REQUIRE(ConnectionTestHelper::GetConnectionResult(conn) == Connection::Result::Pending);
    }
    THEN("error reason is empty") { REQUIRE(ConnectionTestHelper::GetErrorReason(conn).empty()); }
  }
}

SCENARIO("Connection: all state transitions", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a Connection in Idle state") {
    ConnectionTestHelper::SetState(conn, Connection::State::Idle);

    WHEN("state transitions to Connecting") {
      ConnectionTestHelper::SetState(conn, Connection::State::Connecting);
      THEN("state is Connecting") { REQUIRE(ConnectionTestHelper::GetState(conn) == Connection::State::Connecting); }
    }

    ConnectionTestHelper::SetState(conn, Connection::State::Idle);
    WHEN("state transitions to Open") {
      ConnectionTestHelper::SetState(conn, Connection::State::Open);
      THEN("state is Open") { REQUIRE(ConnectionTestHelper::GetState(conn) == Connection::State::Open); }
    }

    ConnectionTestHelper::SetState(conn, Connection::State::Idle);
    WHEN("state transitions to Closing") {
      ConnectionTestHelper::SetState(conn, Connection::State::Closing);
      THEN("state is Closing") { REQUIRE(ConnectionTestHelper::GetState(conn) == Connection::State::Closing); }
    }

    ConnectionTestHelper::SetState(conn, Connection::State::Idle);
    WHEN("state transitions to Closed") {
      ConnectionTestHelper::SetState(conn, Connection::State::Closed);
      THEN("state is Closed") { REQUIRE(ConnectionTestHelper::GetState(conn) == Connection::State::Closed); }
    }

    ConnectionTestHelper::SetState(conn, Connection::State::Idle);
    WHEN("state transitions to Failed") {
      ConnectionTestHelper::SetState(conn, Connection::State::Failed);
      THEN("state is Failed") { REQUIRE(ConnectionTestHelper::GetState(conn) == Connection::State::Failed); }
    }
  }
}

SCENARIO("Connection: MessageReply starts Inactive", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a freshly constructed Connection") {
    auto &reply = ConnectionTestHelper::GetMessageReply(conn);
    THEN("MessageReply is Inactive") { REQUIRE(reply.State() == ConnectionTestHelper::RequestState::Inactive); }
  }
}

SCENARIO("Connection: state() is thread-safe", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());
  std::atomic<bool> done{false};

  GIVEN("a Connection being modified from another thread") {
    WHEN("one thread reads state while another writes") {
      std::thread writer([&]() {
        for (int i = 0; i < 1000 && !done; ++i) {
          ConnectionTestHelper::SetState(conn, static_cast<Connection::State>(i % 6));
        }
      });
      std::thread reader([&]() {
        while (!done) {
          auto s = ConnectionTestHelper::GetState(conn);
          REQUIRE((static_cast<int>(s) >= 0 && static_cast<int>(s) < 6));
        }
      });
      writer.join();
      done = true;
      reader.join();
      THEN("no crashes or invalid states observed") { REQUIRE(true); }
    }
  }
}

SCENARIO("Connection: MessageReply cleanup on disconnect", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("an in-flight request via MessageReply") {
    auto &reply = ConnectionTestHelper::GetMessageReply(conn);
    reply.Activate();

    WHEN("disconnect triggers TryCompleteError (simulating on_fail/on_close path)") {
      reply.TryCompleteError();
      THEN("MessageReply state is Error") { REQUIRE(reply.State() == ConnectionTestHelper::RequestState::Error); }
    }
  }
}

SCENARIO("Connection: MessageReply idle cleanup is no-op", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a Connection with no in-flight request") {
    auto &reply = ConnectionTestHelper::GetMessageReply(conn);
    THEN("state is Inactive") { REQUIRE(reply.State() == ConnectionTestHelper::RequestState::Inactive); }
    WHEN("TryCompleteError is called (simulating Close() on idle connection)") {
      reply.TryCompleteError();
      THEN("state stays Inactive") { REQUIRE(reply.State() == ConnectionTestHelper::RequestState::Inactive); }
    }
  }
}

SCENARIO("Connection: MessageReply double cleanup is idempotent", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("an in-flight request") {
    auto &reply = ConnectionTestHelper::GetMessageReply(conn);
    reply.Activate();

    WHEN("TryCompleteError is called twice (disconnect + close callbacks)") {
      reply.TryCompleteError();
      reply.TryCompleteError();
      THEN("state is Error") { REQUIRE(reply.State() == ConnectionTestHelper::RequestState::Error); }
    }
  }
}

SCENARIO("Connection: MessageReply success then cleanup is no-op", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a completed request") {
    auto &reply = ConnectionTestHelper::GetMessageReply(conn);
    reply.Activate();
    reply.TryCompleteSuccess("done");
    THEN("state is Completed") { REQUIRE(reply.State() == ConnectionTestHelper::RequestState::Completed); }
    WHEN("TryCompleteError is called (e.g. disconnect after reply)") {
      reply.TryCompleteError();
      THEN("state stays Completed") { REQUIRE(reply.State() == ConnectionTestHelper::RequestState::Completed); }
    }
  }
}

SCENARIO("Connection: MessageReply in-flight cleanup path", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("an in-flight request") {
    auto &reply = ConnectionTestHelper::GetMessageReply(conn);
    reply.Activate();
    THEN("state is InFlight") { REQUIRE(reply.State() == ConnectionTestHelper::RequestState::InFlight); }
    THEN("future is not ready") {
      auto fut = reply.Future();
      REQUIRE(fut.wait_for(std::chrono::milliseconds(10)) == std::future_status::timeout);
    }
    WHEN("TryCompleteSuccess is called") {
      reply.TryCompleteSuccess("reply");
      THEN("state is Completed") { REQUIRE(reply.State() == ConnectionTestHelper::RequestState::Completed); }
      THEN("future is ready") {
        auto fut = reply.Future();
        REQUIRE(fut.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready);
      }
      THEN("future.get() returns the message") {
        auto fut = reply.Future();
        REQUIRE(fut.get() == "reply");
      }
    }
  }
}

SCENARIO("Connection: SyncSend returns not_connected when idle with no connection", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a Connection in Idle state with no active connection") {
    THEN("SyncSend returns not_connected error") {
      auto reply = conn.SyncSend("test message");
      REQUIRE(!reply);
      REQUIRE(reply.error().Message() == "Connection is not established");
      REQUIRE(reply.error().Code() == std::make_error_code(std::errc::not_connected));
    }
  }
}

SCENARIO("Connection: SyncSend cancelled when stop token requested", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a Connection with stop token requested") {
    stop_source.request_stop();
    THEN("SyncSend returns operation_canceled immediately") {
      auto reply = conn.SyncSend("test message");
      REQUIRE(!reply);
      REQUIRE(reply.error().Code() == std::make_error_code(std::errc::operation_canceled));
    }
  }
}

SCENARIO("Connection: Connect no-op from Connecting state", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a Connection in Connecting state") {
    ConnectionTestHelper::SetState(conn, Connection::State::Connecting);
    THEN("Connect returns success immediately") {
      auto result = ConnectionTestHelper::Connect(conn);
      REQUIRE(result);
    }
  }
}

SCENARIO("Connection: Connect no-op from Open state", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a Connection in Open state") {
    ConnectionTestHelper::SetState(conn, Connection::State::Open);
    THEN("Connect returns success immediately") {
      auto result = ConnectionTestHelper::Connect(conn);
      REQUIRE(result);
    }
  }
}

SCENARIO("Connection: Connect no-op from Failed state", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a Connection in Failed state") {
    ConnectionTestHelper::SetState(conn, Connection::State::Failed);
    THEN("Connect returns success immediately") {
      auto result = ConnectionTestHelper::Connect(conn);
      REQUIRE(result);
    }
  }
}

SCENARIO("Connection: Close no-op from Closed state", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a Connection in Closed state") {
    ConnectionTestHelper::SetState(conn, Connection::State::Closed);
    THEN("Close returns immediately (no-op)") {
      REQUIRE_NOTHROW(ConnectionTestHelper::Close(conn));
      REQUIRE(ConnectionTestHelper::GetState(conn) == Connection::State::Closed);
    }
  }
}

SCENARIO("Connection: Close no-op from Failed state", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a Connection in Failed state") {
    ConnectionTestHelper::SetState(conn, Connection::State::Failed);
    THEN("Close returns immediately (no-op)") {
      REQUIRE_NOTHROW(ConnectionTestHelper::Close(conn));
      REQUIRE(ConnectionTestHelper::GetState(conn) == Connection::State::Failed);
    }
  }
}

SCENARIO("Connection: Close no-op from Idle state", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a Connection in Idle state") {
    THEN("Close returns immediately (no-op)") {
      REQUIRE_NOTHROW(ConnectionTestHelper::Close(conn));
      REQUIRE(ConnectionTestHelper::GetState(conn) == Connection::State::Idle);
    }
  }
}

SCENARIO("Connection: on_message routes unsolicited message", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a Connection in Open state with no in-flight request") {
    ConnectionTestHelper::SetState(conn, Connection::State::Open);
    auto &reply = ConnectionTestHelper::GetMessageReply(conn);
    THEN("on_message with unsolicited message leaves state unchanged") {
      using ws_msg_manager = websocketpp::message_buffer::alloc::con_msg_manager<
          websocketpp::message_buffer::message<websocketpp::message_buffer::alloc::con_msg_manager>>;
      auto msg_manager = std::make_shared<ws_msg_manager>();
      auto msg = msg_manager->get_message();
      msg->set_payload("unsolicited message");

      websocketpp::connection_hdl hdl;
      conn.on_message(hdl, msg);

      THEN("MessageReply state stays Inactive") {
        REQUIRE(reply.State() == ConnectionTestHelper::RequestState::Inactive);
      }
    }
  }
}

SCENARIO("Connection: on_message dispatches in-flight reply through thread pool", "[pilot][Connection]") {
  auto endpoint = CreateMockEndpoint();
  std::stop_source stop_source;
  Connection conn(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());

  GIVEN("a Connection in Open state with an in-flight request") {
    ConnectionTestHelper::SetState(conn, Connection::State::Open);
    auto &reply = ConnectionTestHelper::GetMessageReply(conn);
    reply.Activate();
    auto fut = reply.Future();

    THEN("on_message dispatches payload to complete the future") {
      using ws_msg_manager = websocketpp::message_buffer::alloc::con_msg_manager<
          websocketpp::message_buffer::message<websocketpp::message_buffer::alloc::con_msg_manager>>;
      auto msg_manager = std::make_shared<ws_msg_manager>();
      auto msg = msg_manager->get_message();
      msg->set_payload("reply payload");

      websocketpp::connection_hdl hdl;
      conn.on_message(hdl, msg);

      // Wait for thread pool to process the dispatch
      std::this_thread::sleep_for(std::chrono::milliseconds(200));

      THEN("future is ready") { REQUIRE(fut.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready); }
      THEN("future.get() returns the payload") { REQUIRE(fut.get() == "reply payload"); }
      THEN("MessageReply state is Completed") {
        REQUIRE(reply.State() == ConnectionTestHelper::RequestState::Completed);
      }
    }
  }
}
