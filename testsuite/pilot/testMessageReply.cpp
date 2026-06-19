#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_exception.hpp>

#include <future>
#include <mutex>
#include <string_view>

namespace {

enum class RequestState {
  Inactive,
  InFlight,
  Completed,
  Error,
};

struct FailedConnectionException : std::exception {
  explicit FailedConnectionException(std::string msg) : what_(std::move(msg)) {}
  const char *what() const noexcept override { return what_.c_str(); }
  std::string what_;
};

struct MessageReply {
  MessageReply() = default;

  void Activate() {
    std::lock_guard lock(m_promise_mutex);
    m_request_state = RequestState::InFlight;
    std::promise<std::string>{}.swap(m_promise);
  }

  void TryCompleteSuccess(std::string_view message) {
    std::lock_guard lock(m_promise_mutex);
    if (m_request_state != RequestState::InFlight)
      return;
    m_request_state = RequestState::Completed;
    m_promise.set_value(std::string{message});
  }

  void TryCompleteError() {
    std::lock_guard lock(m_promise_mutex);
    if (m_request_state != RequestState::InFlight)
      return;
    m_request_state = RequestState::Error;
    m_promise.set_exception(
        std::make_exception_ptr(FailedConnectionException("Connection close while sending a message")));
  }

  void Complete() {
    std::lock_guard lock(m_promise_mutex);
    if (m_request_state != RequestState::InFlight)
      return;
    m_request_state = RequestState::Completed;
  }

  RequestState State() {
    std::lock_guard lock(m_promise_mutex);
    return m_request_state;
  }

  std::future<std::string> Future() { return m_promise.get_future(); }

private:
  RequestState m_request_state = RequestState::Inactive;
  std::promise<std::string> m_promise;
  std::mutex m_promise_mutex;
};

} // namespace

SCENARIO("MessageReply: default construction", "[pilot][MessageReply]") {
  GIVEN("a freshly constructed MessageReply") {
    MessageReply reply;
    THEN("state is Inactive") { REQUIRE(reply.State() == RequestState::Inactive); }
    THEN("Future() returns a valid future") { REQUIRE(reply.Future().valid()); }
  }
}

SCENARIO("MessageReply: Activate transitions to InFlight", "[pilot][MessageReply]") {
  GIVEN("a MessageReply in Inactive state") {
    MessageReply reply;
    WHEN("Activate() is called") {
      reply.Activate();
      THEN("state becomes InFlight") { REQUIRE(reply.State() == RequestState::InFlight); }
    }
  }
}

SCENARIO("MessageReply: TryCompleteSuccess fulfills promise", "[pilot][MessageReply]") {
  GIVEN("a MessageReply in InFlight state") {
    MessageReply reply;
    reply.Activate();
    const std::string payload = "hello world";
    WHEN("TryCompleteSuccess is called with a message") {
      reply.TryCompleteSuccess(payload);
      THEN("state becomes Completed") { REQUIRE(reply.State() == RequestState::Completed); }
      THEN("future is ready") {
        REQUIRE(reply.Future().wait_for(std::chrono::milliseconds(100)) == std::future_status::ready);
      }
      THEN("future.get() returns the message") { REQUIRE(reply.Future().get() == payload); }
    }
  }
}

SCENARIO("MessageReply: TryCompleteError throws from promise", "[pilot][MessageReply]") {
  GIVEN("a MessageReply in InFlight state") {
    MessageReply reply;
    reply.Activate();
    auto fut = reply.Future();
    WHEN("TryCompleteError is called") {
      reply.TryCompleteError();
      THEN("state becomes Error") { REQUIRE(reply.State() == RequestState::Error); }
      THEN("future.get() throws FailedConnectionException") {
        REQUIRE(fut.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready);
        REQUIRE_THROWS_AS(fut.get(), FailedConnectionException);
      }
    }
  }
}

SCENARIO("MessageReply: TryComplete* is idempotent", "[pilot][MessageReply]") {
  GIVEN("a MessageReply in Inactive state") {
    MessageReply reply;
    WHEN("TryCompleteSuccess is called without Activate") {
      reply.TryCompleteSuccess("ignored");
      THEN("state stays Inactive") { REQUIRE(reply.State() == RequestState::Inactive); }
      THEN("future is not ready") {
        REQUIRE(reply.Future().wait_for(std::chrono::milliseconds(10)) == std::future_status::timeout);
      }
    }
    WHEN("TryCompleteError is called without Activate") {
      reply.TryCompleteError();
      THEN("state stays Inactive") { REQUIRE(reply.State() == RequestState::Inactive); }
    }
  }
}

SCENARIO("MessageReply: double completion is safe", "[pilot][MessageReply]") {
  GIVEN("a MessageReply in InFlight state") {
    MessageReply reply;
    reply.Activate();
    auto fut = reply.Future();
    WHEN("TryCompleteSuccess is called twice") {
      reply.TryCompleteSuccess("first");
      reply.TryCompleteSuccess("second");
      THEN("state is Completed (not changed by second call)") { REQUIRE(reply.State() == RequestState::Completed); }
      THEN("future returns the first message") {
        REQUIRE(fut.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready);
        REQUIRE(fut.get() == "first");
      }
    }
  }
}

SCENARIO("MessageReply: Complete without fulfillment", "[pilot][MessageReply]") {
  GIVEN("a MessageReply in InFlight state") {
    MessageReply reply;
    reply.Activate();
    auto fut = reply.Future();
    WHEN("Complete() is called without setting promise value") {
      reply.Complete();
      THEN("state becomes Completed") { REQUIRE(reply.State() == RequestState::Completed); }
      THEN("future is not ready (promise was never fulfilled)") {
        REQUIRE(fut.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout);
      }
    }
  }
}

SCENARIO("MessageReply: error after success is a no-op", "[pilot][MessageReply]") {
  GIVEN("a MessageReply that was already completed successfully") {
    MessageReply reply;
    reply.Activate();
    auto fut = reply.Future();
    reply.TryCompleteSuccess("ok");
    WHEN("TryCompleteError is called afterwards") {
      reply.TryCompleteError();
      THEN("state stays Completed") { REQUIRE(reply.State() == RequestState::Completed); }
      THEN("future still returns the success message") {
        REQUIRE(fut.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready);
        REQUIRE(fut.get() == "ok");
      }
    }
  }
}

SCENARIO("MessageReply: success after error is a no-op", "[pilot][MessageReply]") {
  GIVEN("a MessageReply that was already errored") {
    MessageReply reply;
    reply.Activate();
    auto fut = reply.Future();
    reply.TryCompleteError();
    WHEN("TryCompleteSuccess is called afterwards") {
      reply.TryCompleteSuccess("should be ignored");
      THEN("state stays Error") { REQUIRE(reply.State() == RequestState::Error); }
      THEN("future still throws") {
        REQUIRE(fut.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready);
        REQUIRE_THROWS_AS(fut.get(), FailedConnectionException);
      }
    }
  }
}

SCENARIO("MessageReply: Activate resets promise", "[pilot][MessageReply]") {
  GIVEN("a MessageReply that was already used") {
    MessageReply reply;
    reply.Activate();
    reply.TryCompleteSuccess("first round");
    WHEN("Activate is called again") {
      reply.Activate();
      THEN("state returns to InFlight") { REQUIRE(reply.State() == RequestState::InFlight); }
      THEN("it can be completed again") {
        auto fut2 = reply.Future();
        reply.TryCompleteSuccess("second round");
        REQUIRE(fut2.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready);
        REQUIRE(fut2.get() == "second round");
      }
    }
  }
}

SCENARIO("MessageReply: disconnect cleanup clears in-flight request", "[pilot][MessageReply]") {
  GIVEN("an in-flight request") {
    MessageReply reply;
    reply.Activate();
    auto fut = reply.Future();
    WHEN("disconnect triggers TryCompleteError") {
      reply.TryCompleteError();
      THEN("state transitions to Error") { REQUIRE(reply.State() == RequestState::Error); }
      THEN("future is ready") { REQUIRE(fut.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready); }
      THEN("future.get() throws FailedConnectionException") { REQUIRE_THROWS_AS(fut.get(), FailedConnectionException); }
    }
  }
}

SCENARIO("MessageReply: cancel cleanup clears in-flight request", "[pilot][MessageReply]") {
  GIVEN("an in-flight request") {
    MessageReply reply;
    reply.Activate();
    auto fut = reply.Future();
    WHEN("cancel triggers TryCompleteError (same path as disconnect)") {
      reply.TryCompleteError();
      THEN("state is Error") { REQUIRE(reply.State() == RequestState::Error); }
      THEN("future throws") {
        REQUIRE(fut.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready);
        REQUIRE_THROWS_AS(fut.get(), FailedConnectionException);
      }
    }
  }
}

SCENARIO("MessageReply: double cleanup is safe", "[pilot][MessageReply]") {
  GIVEN("an in-flight request") {
    MessageReply reply;
    reply.Activate();
    auto fut = reply.Future();
    WHEN("TryCompleteError is called twice (disconnect + close)") {
      reply.TryCompleteError();
      reply.TryCompleteError();
      THEN("state is Error (second call is no-op)") { REQUIRE(reply.State() == RequestState::Error); }
      THEN("future throws exactly once") {
        REQUIRE(fut.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready);
        REQUIRE_THROWS_AS(fut.get(), FailedConnectionException);
      }
    }
  }
}

SCENARIO("MessageReply: idle cleanup is no-op", "[pilot][MessageReply]") {
  GIVEN("an idle MessageReply (no request in flight)") {
    MessageReply reply;
    THEN("state is Inactive") { REQUIRE(reply.State() == RequestState::Inactive); }
    WHEN("TryCompleteError is called (e.g. Close() called without active send)") {
      reply.TryCompleteError();
      THEN("state stays Inactive") { REQUIRE(reply.State() == RequestState::Inactive); }
      THEN("future is not ready") {
        REQUIRE(reply.Future().wait_for(std::chrono::milliseconds(10)) == std::future_status::timeout);
      }
    }
  }
}

SCENARIO("MessageReply: cleanup after success is no-op", "[pilot][MessageReply]") {
  GIVEN("a completed request") {
    MessageReply reply;
    reply.Activate();
    reply.TryCompleteSuccess("done");
    WHEN("TryCompleteError is called (e.g. disconnect after reply)") {
      reply.TryCompleteError();
      THEN("state stays Completed") { REQUIRE(reply.State() == RequestState::Completed); }
    }
  }
}

SCENARIO("MessageReply: error is self-contained in promise", "[pilot][MessageReply]") {
  GIVEN("an in-flight request") {
    MessageReply reply;
    reply.Activate();
    auto fut = reply.Future();
    WHEN("TryCompleteError is called") {
      reply.TryCompleteError();
      THEN("the exception type is FailedConnectionException") {
        bool caught_correct_type = false;
        try {
          fut.get();
        } catch (const FailedConnectionException &) {
          caught_correct_type = true;
        }
        REQUIRE(caught_correct_type);
      }
    }
  }
}
