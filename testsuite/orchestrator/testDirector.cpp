// c++ headers
#include <memory>

// external dependencies
#include <catch2/catch_test_macros.hpp>
#include <catch2/trompeloeil.hpp>
#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>

// our headers
#include "db/backends/Backend.h"
#include "db/harness/Harness.h"
#include "orchestrator/Director.h"

using namespace PMS;
using namespace PMS::Orchestrator;
using namespace PMS::DB;

namespace PMS::Tests::Orchestrator {

// ---------------------------------------------------------------------------
// MockBackend
// ---------------------------------------------------------------------------

class MockBackend : public trompeloeil::mock_interface<PMS::DB::Backend> {
public:
  MAKE_MOCK0(Connect, auto(void)->ErrorOr<void>, override);
  MAKE_MOCK2(Connect, auto(std::string_view, std::string_view)->ErrorOr<void>, override);
  IMPLEMENT_MOCK0(SetupIfNeeded);
  IMPLEMENT_MOCK1(RunQuery);
  IMPLEMENT_MOCK2(BulkWrite);
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static exec::static_thread_pool g_pool{2};

template <typename T> auto run_async(stdexec::task<T> task) {
  return stdexec::sync_wait(stdexec::on(g_pool.get_scheduler(), std::move(task)));
}

// Create a Director and return raw pointers to the underlying mocks.
struct Fixture {
  MockBackend *frontMock;
  MockBackend *backMock;
  std::unique_ptr<MockBackend> frontOwned;
  std::unique_ptr<MockBackend> backOwned;

  Fixture() {
    frontOwned = std::make_unique<MockBackend>();
    backOwned = std::make_unique<MockBackend>();
    frontMock = frontOwned.get();
    backMock = backOwned.get();
  }

  Director make_director() {
    return Director{std::make_unique<DB::Harness>(std::move(frontOwned)),
                    std::make_unique<DB::Harness>(std::move(backOwned))};
  }
};

// ---------------------------------------------------------------------------
// ValidateTaskToken
// ---------------------------------------------------------------------------

SCENARIO("Director: ValidateTaskToken", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  GIVEN("a task created in memory") {
    // prime the task into memory via CreateTask
    ALLOW_CALL(*f.backMock, RunQuery(trompeloeil::_)).RETURN(json{});
    auto create_r = run_async(director.CreateTask("myTask"));
    REQUIRE(create_r.has_value());
    auto [token_result] = create_r.value();
    REQUIRE(token_result.has_value());
    const std::string token = token_result.value();

    WHEN("the correct token is provided") {
      THEN("OperationResult::Success is returned") {
        REQUIRE(director.ValidateTaskToken("myTask", token) == IDirector::OperationResult::Success);
      }
    }

    WHEN("a wrong token is provided") {
      THEN("OperationResult::ProcessError is returned") {
        REQUIRE(director.ValidateTaskToken("myTask", "wrong") == IDirector::OperationResult::ProcessError);
      }
    }
  }

  GIVEN("a task name that was never created") {
    WHEN("ValidateTaskToken is called") {
      THEN("OperationResult::DatabaseError is returned") {
        REQUIRE(director.ValidateTaskToken("unknown", "any") == IDirector::OperationResult::DatabaseError);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// CreateTask
// ---------------------------------------------------------------------------

SCENARIO("Director: CreateTask", "[Director]") {
  GIVEN("a fresh director with a working backend") {
    Fixture f;
    auto director = f.make_director();

    WHEN("the DB insert succeeds") {
      REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Insert>(_1))
          .RETURN(json{});

      auto result = run_async(director.CreateTask("task1"));

      THEN("a non-empty token is returned") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
        REQUIRE_FALSE(r.value().empty());
      }
    }

    WHEN("the DB insert fails") {
      REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Insert>(_1))
          .RETURN(make_error(std::errc::io_error, "DB error"));

      auto result = run_async(director.CreateTask("task1"));

      THEN("an error is propagated") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE_FALSE(r.has_value());
      }
    }
  }

  GIVEN("a director where the task was already created") {
    Fixture f;
    auto director = f.make_director();

    ALLOW_CALL(*f.backMock, RunQuery(trompeloeil::_)).RETURN(json{});
    run_async(director.CreateTask("task1")); // prime m_tasks

    WHEN("CreateTask is called again with the same name") {
      // NOTE: Director mutates m_tasks before the DB call, so duplicate detection
      // is purely in-memory and returns early without a DB call.
      THEN("an error is returned without a DB round-trip") {
        auto result = run_async(director.CreateTask("task1"));
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE_FALSE(r.has_value());
      }
    }
  }
}

// ---------------------------------------------------------------------------
// AddTaskDependency
// ---------------------------------------------------------------------------

SCENARIO("Director: AddTaskDependency", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  // Pre-create both tasks so they're in m_tasks.
  ALLOW_CALL(*f.backMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<PMS::DB::Queries::Insert>(_1))
      .RETURN(json{});
  run_async(director.CreateTask("A"));
  run_async(director.CreateTask("B"));

  GIVEN("a valid dependency between two existing tasks") {
    WHEN("the DB update succeeds") {
      REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Update>(_1))
          .RETURN(json{{"matched_count", 1}, {"modified_count", 1}});

      auto result = run_async(director.AddTaskDependency("A", "B"));

      THEN("void success is returned") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
      }
    }

    WHEN("the DB update fails") {
      REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Update>(_1))
          .RETURN(make_error(std::errc::io_error, "DB error"));

      auto result = run_async(director.AddTaskDependency("A", "B"));

      THEN("an error is propagated") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE_FALSE(r.has_value());
      }
    }
  }
}

// ---------------------------------------------------------------------------
// ClearTask
// ---------------------------------------------------------------------------

SCENARIO("Director: ClearTask with deleteTask=true", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  // Pre-create the task.
  ALLOW_CALL(*f.backMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<PMS::DB::Queries::Insert>(_1))
      .RETURN(json{});
  run_async(director.CreateTask("myTask"));

  GIVEN("all DB deletes succeed") {
    // when_all deletes jobs from back and front DB (order not guaranteed)
    REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
        .WITH(std::holds_alternative<PMS::DB::Queries::Delete>(_1))
        .TIMES(2)
        .RETURN(json{{"deleted_count", 0}});
    REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
        .WITH(std::holds_alternative<PMS::DB::Queries::Delete>(_1))
        .RETURN(json{{"deleted_count", 0}});

    auto result = run_async(director.ClearTask("myTask", true));

    THEN("void success is returned") {
      REQUIRE(result.has_value());
      auto [r] = result.value();
      REQUIRE(r.has_value());
    }
  }

  GIVEN("a failing job-delete on the backend") {
    ALLOW_CALL(*f.backMock, RunQuery(trompeloeil::_))
        .WITH(std::holds_alternative<PMS::DB::Queries::Delete>(_1))
        .RETURN(make_error(std::errc::io_error, "DB error"));
    ALLOW_CALL(*f.frontMock, RunQuery(trompeloeil::_))
        .WITH(std::holds_alternative<PMS::DB::Queries::Delete>(_1))
        .RETURN(json{{"deleted_count", 0}});

    auto result = run_async(director.ClearTask("myTask", true));

    THEN("an error is propagated") {
      REQUIRE(result.has_value());
      auto [r] = result.value();
      REQUIRE_FALSE(r.has_value());
    }
  }
}

SCENARIO("Director: ClearTask with deleteTask=false", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  ALLOW_CALL(*f.backMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<PMS::DB::Queries::Insert>(_1))
      .RETURN(json{});
  run_async(director.CreateTask("myTask"));

  GIVEN("all DB deletes succeed") {
    // Only job deletes — no task collection delete.
    REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
        .WITH(std::holds_alternative<PMS::DB::Queries::Delete>(_1))
        .TIMES(1)
        .RETURN(json{{"deleted_count", 0}});
    REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
        .WITH(std::holds_alternative<PMS::DB::Queries::Delete>(_1))
        .RETURN(json{{"deleted_count", 0}});

    auto result = run_async(director.ClearTask("myTask", false));

    THEN("void success is returned") {
      REQUIRE(result.has_value());
      auto [r] = result.value();
      REQUIRE(r.has_value());
    }
  }
}

// ---------------------------------------------------------------------------
// RegisterNewPilot
// ---------------------------------------------------------------------------

SCENARIO("Director: RegisterNewPilot", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  // Pre-create a task so its token is known.
  std::string task_token;
  {
    ALLOW_CALL(*f.backMock, RunQuery(trompeloeil::_))
        .WITH(std::holds_alternative<PMS::DB::Queries::Insert>(_1))
        .RETURN(json{});
    auto r = run_async(director.CreateTask("validTask"));
    auto [er] = r.value();
    task_token = er.value();
  }

  GIVEN("all provided tasks are valid") {
    WHEN("the front-DB insert succeeds") {
      REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Insert>(_1))
          .RETURN(json{});

      std::vector<std::pair<std::string, std::string>> tasks = {{"validTask", task_token}};
      auto result = run_async(director.RegisterNewPilot("uuid-1", "alice", tasks, {}, json{}));

      THEN("validTasks contains the task name") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
        REQUIRE(r.value().validTasks.size() == 1);
        REQUIRE(r.value().validTasks[0] == "validTask");
        REQUIRE(r.value().invalidTasks.empty());
      }
    }
  }

  GIVEN("a mix of valid and invalid tasks") {
    WHEN("the front-DB insert succeeds") {
      REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Insert>(_1))
          .RETURN(json{});

      std::vector<std::pair<std::string, std::string>> tasks = {{"validTask", task_token}, {"unknownTask", "badtoken"}};
      auto result = run_async(director.RegisterNewPilot("uuid-2", "alice", tasks, {}, json{}));

      THEN("only valid tasks appear in validTasks") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
        REQUIRE(r.value().validTasks.size() == 1);
        REQUIRE(r.value().invalidTasks.size() == 1);
      }
    }
  }

  GIVEN("the front-DB insert fails") {
    REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
        .WITH(std::holds_alternative<PMS::DB::Queries::Insert>(_1))
        .RETURN(make_error(std::errc::io_error, "DB error"));

    std::vector<std::pair<std::string, std::string>> tasks = {{"validTask", task_token}};
    auto result = run_async(director.RegisterNewPilot("uuid-3", "alice", tasks, {}, json{}));

    THEN("an error is returned") {
      REQUIRE(result.has_value());
      auto [r] = result.value();
      REQUIRE_FALSE(r.has_value());
    }
  }
}

// ---------------------------------------------------------------------------
// DeleteHeartBeat
// ---------------------------------------------------------------------------

SCENARIO("Director: DeleteHeartBeat", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  GIVEN("the pilot is not known in the active cache") {
    WHEN("the front-DB delete succeeds") {
      REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Delete>(_1))
          .RETURN(json{{"deleted_count", 1}});

      auto result = run_async(director.DeleteHeartBeat("uuid-unknown"));

      THEN("void success is returned") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
      }
    }

    WHEN("the front-DB delete fails") {
      REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Delete>(_1))
          .RETURN(make_error(std::errc::io_error, "DB error"));

      auto result = run_async(director.DeleteHeartBeat("uuid-unknown"));

      THEN("an error is propagated") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE_FALSE(r.has_value());
      }
    }
  }
}

// ---------------------------------------------------------------------------
// ResetFailedJobs
// ---------------------------------------------------------------------------

SCENARIO("Director: ResetFailedJobs", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  // Pre-create the task so m_tasks is populated.
  ALLOW_CALL(*f.backMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<PMS::DB::Queries::Insert>(_1))
      .RETURN(json{});
  run_async(director.CreateTask("myTask"));

  GIVEN("both DBs succeed and UpdateTaskCounts can query counts") {
    // when_all Update calls on front and back (order not guaranteed)
    ALLOW_CALL(*f.frontMock, RunQuery(trompeloeil::_))
        .WITH(std::holds_alternative<PMS::DB::Queries::Update>(_1))
        .RETURN(json{{"matched_count", 2}, {"modified_count", 2}});
    ALLOW_CALL(*f.backMock, RunQuery(trompeloeil::_))
        .WITH(std::holds_alternative<PMS::DB::Queries::Update>(_1))
        .RETURN(json{{"matched_count", 2}, {"modified_count", 2}});
    // UpdateTaskCounts calls Count for each JobStatus value (10 values).
    ALLOW_CALL(*f.backMock, RunQuery(trompeloeil::_))
        .WITH(std::holds_alternative<PMS::DB::Queries::Count>(_1))
        .RETURN(json{{"count", 0}});

    auto result = run_async(director.ResetFailedJobs("myTask"));

    THEN("void success is returned") {
      REQUIRE(result.has_value());
      auto [r] = result.value();
      REQUIRE(r.has_value());
    }
  }
}

} // namespace PMS::Tests::Orchestrator
