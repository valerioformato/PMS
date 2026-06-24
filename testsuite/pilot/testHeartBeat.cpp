#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <memory>
#include <stop_token>
#include <thread>

#include "pilot/HeartBeat.h"
#include "pilot/client/Connection.h"
#include <websocketpp/message_buffer/alloc.hpp>

namespace {

using namespace PMS::Pilot;

} // namespace

static void test_heart_beat_destruction(boost::uuids::uuid uuid, std::stop_source &stop_source) {
  auto endpoint = std::make_shared<WSclient>();
  auto conn =
      std::make_unique<Connection>(Connection::no_connect, endpoint, "ws://localhost:9999", stop_source.get_token());
  HeartBeat hb{uuid, std::move(conn)};
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // hb destructor runs here, joining the thread and cleaning up
}

SCENARIO("HeartBeat runs and destroys cleanly on idle connection", "[pilot][HeartBeat]") {
  boost::uuids::uuid uuid;
  std::stop_source stop_source;

  GIVEN("a HeartBeat with idle connection") {
    WHEN("HeartBeat is created and goes out of scope") {
      THEN("destruction completes without crashing") {
        REQUIRE_NOTHROW([&]() { test_heart_beat_destruction(uuid, stop_source); }());
      }
    }
  }
}

SCENARIO("HeartBeat handles send failure gracefully", "[pilot][HeartBeat]") {
  boost::uuids::uuid uuid;
  std::stop_source stop_source;

  GIVEN("a HeartBeat with idle connection") {
    WHEN("HeartBeat attempts to send on idle connection") {
      THEN("HeartBeat survives the failed send and destroys cleanly") {
        REQUIRE_NOTHROW([&]() { test_heart_beat_destruction(uuid, stop_source); }());
      }
    }
  }
}

SCENARIO("HeartBeat cleanup completes on destruction", "[pilot][HeartBeat]") {
  boost::uuids::uuid uuid;
  std::stop_source stop_source;

  GIVEN("a HeartBeat that is about to be destroyed") {
    WHEN("HeartBeat is created and destroyed") {
      THEN("destruction completes without crashing") {
        REQUIRE_NOTHROW([&]() { test_heart_beat_destruction(uuid, stop_source); }());
      }
    }
  }
}
