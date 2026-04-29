// c++ headers
#include <algorithm>
#include <thread>
#include <vector>

// external dependencies
#include <catch2/catch_test_macros.hpp>

// our headers
#include "common/queue.h"

namespace PMS::Tests::Common {

// ---------------------------------------------------------------------------
// PMS::ts_queue (lock-free MPSC queue)
// ---------------------------------------------------------------------------

SCENARIO("ts_queue: basic push/pop and empty", "[Queue]") {
  GIVEN("An empty queue") {
    ts_queue<int> q{16};
    THEN("empty() returns true") { REQUIRE(q.empty()); }

    WHEN("One item is pushed") {
      q.push(42);
      THEN("empty() returns false") { REQUIRE_FALSE(q.empty()); }

      AND_WHEN("The item is popped") {
        int val = q.pop();
        THEN("The popped value matches") { REQUIRE(val == 42); }
        THEN("The queue is empty again") { REQUIRE(q.empty()); }
      }
    }
  }
}

SCENARIO("ts_queue: consume_all drains the queue", "[Queue]") {
  GIVEN("A queue with several items pushed in order") {
    ts_queue<int> q{16};
    q.push(1);
    q.push(2);
    q.push(3);

    WHEN("consume_all is called") {
      auto items = q.consume_all();

      THEN("All pushed items are returned") {
        REQUIRE(items.size() == 3);
        std::sort(items.begin(), items.end());
        REQUIRE(items == std::vector<int>{1, 2, 3});
      }

      THEN("The queue is empty afterwards") { REQUIRE(q.empty()); }
    }
  }
}

SCENARIO("ts_queue: concurrent producers, single consumer", "[Queue]") {
  GIVEN("A queue with capacity for all items") {
    constexpr int nThreads = 8;
    constexpr int itemsPerThread = 50;
    constexpr int total = nThreads * itemsPerThread;
    ts_queue<int> q{total + 1};

    WHEN("N threads each push M unique items, then all threads are joined") {
      std::vector<std::thread> threads;
      for (int i = 0; i < nThreads; ++i) {
        threads.emplace_back([&q, i]() {
          for (int j = 0; j < itemsPerThread; ++j) {
            q.push(i * itemsPerThread + j);
          }
        });
      }
      for (auto &t : threads)
        t.join();

      THEN("consume_all recovers all N*M items (regardless of order)") {
        auto items = q.consume_all();
        REQUIRE(items.size() == static_cast<std::size_t>(total));

        std::sort(items.begin(), items.end());
        for (int i = 0; i < total; ++i) {
          REQUIRE(items[i] == i);
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// PMS::old::ts_queue (mutex-based queue)
// ---------------------------------------------------------------------------

SCENARIO("old::ts_queue: basic push/pop/size/empty", "[Queue][old]") {
  GIVEN("An empty queue") {
    old::ts_queue<int> q;
    THEN("empty() returns true") { REQUIRE(q.empty()); }
    THEN("size() returns 0") { REQUIRE(q.size() == 0); }

    WHEN("Items are pushed") {
      q.push(10);
      q.push(20);
      THEN("size() reflects the count") { REQUIRE(q.size() == 2); }
      THEN("empty() returns false") { REQUIRE_FALSE(q.empty()); }
    }
  }
}

SCENARIO("old::ts_queue: consume() removes and returns front element", "[Queue][old]") {
  GIVEN("A queue with two items") {
    old::ts_queue<int> q;
    q.push(1);
    q.push(2);

    WHEN("consume() is called") {
      int val = q.consume();
      THEN("The front element is returned") { REQUIRE(val == 1); }
      THEN("The queue shrinks by one") { REQUIRE(q.size() == 1); }
    }
  }
}

SCENARIO("old::ts_queue: consume_all drains the queue", "[Queue][old]") {
  GIVEN("A queue with several items") {
    old::ts_queue<int> q;
    q.push(3);
    q.push(1);
    q.push(2);

    WHEN("consume_all is called") {
      auto items = q.consume_all();
      THEN("All items are returned in insertion order") { REQUIRE(items == std::vector<int>{3, 1, 2}); }
      THEN("The queue is empty afterwards") { REQUIRE(q.empty()); }
    }
  }
}

} // namespace PMS::Tests::Common
