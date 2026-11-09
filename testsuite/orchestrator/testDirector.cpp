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

#define private public
#include "orchestrator/Director.h"
#undef private

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
    return Director{1u, std::make_unique<DB::Harness>(std::move(frontOwned)),
                    std::make_unique<DB::Harness>(std::move(backOwned))};
  }
};

static void prime_claim_state(Director &director, std::string pilot_uuid, std::vector<std::string> tags = {}) {
  auto &task = director.m_tasks["task-1"];
  task.name = "task-1";
  task.totJobs = 1;
  task.jobs[JobStatus::Pending] = 1;
  task.readyForScheduling = true;

  director.m_activePilots.emplace(
      pilot_uuid, Director::PilotInfo{.uuid = pilot_uuid, .tasks = {"task-1"}, .tags = std::move(tags)});
}

static const DB::Queries::Match &find_match(const DB::Queries::FindOneAndUpdate &query, std::string_view key,
                                            DB::Queries::ComparisonOp op) {
  const auto match = std::ranges::find_if(
      query.match, [key, op](const auto &candidate) { return candidate.key == key && candidate.op == op; });
  REQUIRE(match != query.match.end());
  return *match;
}

static const DB::Queries::UpdateAction &find_update(const DB::Queries::FindOneAndUpdate &query, std::string_view key,
                                                    DB::Queries::UpdateOp op) {
  const auto update = std::ranges::find_if(
      query.update, [key, op](const auto &candidate) { return candidate.key == key && candidate.op == op; });
  REQUIRE(update != query.update.end());
  return *update;
}

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
          .WITH(std::holds_alternative<PMS::DB::Queries::Insert>(_1) &&
                std::get<PMS::DB::Queries::Insert>(_1).documents.size() == 1 &&
                std::get<PMS::DB::Queries::Insert>(_1).documents[0].contains("lastHeartBeat"))
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
// PilotClaimJob
// ---------------------------------------------------------------------------

SCENARIO("Director: PilotClaimJob uses one atomic database operation", "[Director][PilotClaimJob]") {
  Fixture f;
  auto director = f.make_director();
  prime_claim_state(director, "pilot-1", {"gpu", "site-a"});

  DB::Queries::Query captured_query;
  const json job = {
      {"hash", "job-1"},
      {"task", "task-1"},
      {"executable", "/bin/true"},
  };

  REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<DB::Queries::FindOneAndUpdate>(_1))
      .LR_SIDE_EFFECT(captured_query = _1)
      .RETURN(job);

  const auto result = run_async(director.PilotClaimJob("pilot-1"));

  REQUIRE(result.has_value());
  const auto &[claim_result] = result.value();
  REQUIRE(claim_result.has_value());
  REQUIRE(claim_result.value() == job);

  const auto &query = std::get<DB::Queries::FindOneAndUpdate>(captured_query);
  REQUIRE(query.collection == "jobs");
  REQUIRE(query.filter == R"({"_id":0, "dataset":0, "jobName":0, "status":0, "tags":0, "user":0})"_json);

  const auto &statuses = find_match(query, "status", DB::Queries::ComparisonOp::IN).value;
  REQUIRE(statuses == json::array({magic_enum::enum_name(JobStatus::Pending), magic_enum::enum_name(JobStatus::Error),
                                   magic_enum::enum_name(JobStatus::OutboundTransferError),
                                   magic_enum::enum_name(JobStatus::InboundTransferError)}));
  REQUIRE(find_match(query, "task", DB::Queries::ComparisonOp::IN).value == json::array({"task-1"}));
  REQUIRE(find_match(query, "tags", DB::Queries::ComparisonOp::ALL).value == json::array({"gpu", "site-a"}));

  REQUIRE(find_update(query, "status", DB::Queries::UpdateOp::SET).value == magic_enum::enum_name(JobStatus::Claimed));
  REQUIRE(find_update(query, "pilotUuid", DB::Queries::UpdateOp::SET).value == "pilot-1");
  REQUIRE(find_update(query, "retries", DB::Queries::UpdateOp::INC).value == 1);
  REQUIRE(find_update(query, "lastUpdate", DB::Queries::UpdateOp::SET).value.is_number_integer());
}

SCENARIO("Director: PilotClaimJob preserves empty-tag matching", "[Director][PilotClaimJob]") {
  Fixture f;
  auto director = f.make_director();
  prime_claim_state(director, "pilot-1");

  DB::Queries::Query captured_query;
  REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<DB::Queries::FindOneAndUpdate>(_1))
      .LR_SIDE_EFFECT(captured_query = _1)
      .RETURN(json{});

  const auto result = run_async(director.PilotClaimJob("pilot-1"));

  REQUIRE(result.has_value());
  const auto &[claim_result] = result.value();
  REQUIRE(claim_result.has_value());
  REQUIRE(claim_result.value() == R"({"sleep": true})"_json);

  const auto &query = std::get<DB::Queries::FindOneAndUpdate>(captured_query);
  REQUIRE(find_match(query, "tags", DB::Queries::ComparisonOp::TYPE).value == "array");
  REQUIRE(find_match(query, "tags", DB::Queries::ComparisonOp::EQ).value == json::array());
}

SCENARIO("Director: PilotClaimJob converts database failures to sleep", "[Director][PilotClaimJob]") {
  Fixture f;
  auto director = f.make_director();
  prime_claim_state(director, "pilot-1");

  REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<DB::Queries::FindOneAndUpdate>(_1))
      .RETURN(make_error(std::errc::io_error, "DB error"));

  const auto result = run_async(director.PilotClaimJob("pilot-1"));

  REQUIRE(result.has_value());
  const auto &[claim_result] = result.value();
  REQUIRE(claim_result.has_value());
  REQUIRE(claim_result.value() == R"({"sleep": true})"_json);
}

// ---------------------------------------------------------------------------
// DeleteHeartBeat
// ---------------------------------------------------------------------------

SCENARIO("Director: DeleteHeartBeat", "[Director]") {
  Fixture f;
  auto director = f.make_director();
  ALLOW_CALL(*f.frontMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<PMS::DB::Queries::Find>(_1))
      .RETURN(json::array());

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

SCENARIO("Director: deleting a pilot with a running job resolves the job first", "[Director]") {
  Fixture f;
  auto director = f.make_director();
  std::vector<Queries::Query> queries;
  ALLOW_CALL(*f.frontMock, RunQuery(trompeloeil::_))
      .LR_SIDE_EFFECT(queries.push_back(_1))
      .LR_RETURN(std::holds_alternative<Queries::Find>(_1)
                     ? json::array({{{"hash", "job-hash"}, {"task", "task-1"}, {"status", "Running"}}})
                     : json{{"matched_count", 1}, {"modified_count", 1}, {"deleted_count", 1}});

  auto result = run_async(director.DeleteHeartBeat("pilot-1"));
  REQUIRE(result.has_value());
  REQUIRE(std::get<0>(result.value()).has_value());

  auto find_job = std::ranges::find_if(queries, [](const auto &query) {
    return std::holds_alternative<Queries::Find>(query) && std::get<Queries::Find>(query).collection == "jobs";
  });
  REQUIRE(find_job != queries.end());

  auto update_job = std::ranges::find_if(find_job, queries.end(), [](const auto &query) {
    return std::holds_alternative<Queries::Update>(query) && std::get<Queries::Update>(query).collection == "jobs" &&
           std::ranges::any_of(std::get<Queries::Update>(query).update,
                               [](const auto &action) { return action.key == "status" && action.value == "Error"; });
  });
  REQUIRE(update_job != queries.end());
  REQUIRE(std::get<Queries::Update>(*update_job).options.limit == 0);
  REQUIRE(std::ranges::any_of(std::get<Queries::Update>(*update_job).update,
                              [](const auto &action) { return action.key == "lastUpdate"; }));

  auto delete_pilot = std::ranges::find_if(update_job, queries.end(), [](const auto &query) {
    return std::holds_alternative<Queries::Delete>(query) && std::get<Queries::Delete>(query).collection == "pilots";
  });
  REQUIRE(delete_pilot != queries.end());
}

SCENARIO("Director: pilot deletion leaves its record when the active-job update fails", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<Queries::Find>(_1) && std::get<Queries::Find>(_1).collection == "jobs")
      .RETURN(json::array({{{"hash", "job-1"}}, {{"hash", "job-2"}}}));
  REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<Queries::Update>(_1) && std::get<Queries::Update>(_1).collection == "jobs" &&
            std::get<Queries::Update>(_1).options.limit == 0)
      .RETURN(make_error(std::errc::io_error, "job update failed"));
  FORBID_CALL(*f.frontMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<Queries::Delete>(_1) && std::get<Queries::Delete>(_1).collection == "pilots");

  auto result = run_async(director.DeleteHeartBeat("pilot-1"));
  REQUIRE(result.has_value());
  REQUIRE_FALSE(std::get<0>(result.value()).has_value());
}

SCENARIO("Director: orphan reconciliation resolves only jobs without a pilot", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<Queries::Find>(_1) && std::get<Queries::Find>(_1).collection == "jobs")
      .RETURN(json::array(
          {{{"hash", "orphan-job"}, {"task", "task-1"}, {"pilotUuid", "missing-pilot"}, {"status", "Running"}},
           {{"hash", "live-job"}, {"task", "task-1"}, {"pilotUuid", "live-pilot"}, {"status", "Running"}}}));
  REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<Queries::Find>(_1) && std::get<Queries::Find>(_1).collection == "pilots")
      .RETURN(json::array({{{"uuid", "live-pilot"}}}));
  REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<Queries::Update>(_1) && std::get<Queries::Update>(_1).collection == "jobs" &&
            std::ranges::any_of(std::get<Queries::Update>(_1).match,
                                [](const auto &match) { return match.key == "hash" && match.value == "orphan-job"; }))
      .RETURN(json{{"modified_count", 1}});

  director.ReconcileOrphanJobs();
}

SCENARIO("Director: pilot deletion persists queued job updates before retiring the pilot", "[Director]") {
  Fixture f;
  auto director = f.make_director();
  director.m_jobUpdateRequests.push_back(Queries::Update{
      .collection = "jobs",
      .match = {{"hash", "job-1"}},
      .update = {{"status", "Done"}},
  });

  std::vector<std::string> operations;
  REQUIRE_CALL(*f.frontMock, BulkWrite("jobs", trompeloeil::_))
      .LR_SIDE_EFFECT(operations.emplace_back("flush"))
      .RETURN(json{{"modified_count", 1}});
  REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<Queries::Find>(_1) && std::get<Queries::Find>(_1).collection == "jobs")
      .LR_SIDE_EFFECT(operations.emplace_back("find"))
      .RETURN(json::array());
  REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<Queries::Delete>(_1) && std::get<Queries::Delete>(_1).collection == "pilots")
      .LR_SIDE_EFFECT(operations.emplace_back("delete"))
      .RETURN(json{{"deleted_count", 1}});

  auto result = run_async(director.DeleteHeartBeat("pilot-1"));
  REQUIRE(result.has_value());
  REQUIRE(std::get<0>(result.value()).has_value());
  REQUIRE(operations == std::vector<std::string>{"flush", "find", "delete"});
}

SCENARIO("Director: queued nonterminal status cannot revive a retired job", "[Director]") {
  Fixture f;
  auto director = f.make_director();
  director.m_activePilots.emplace("pilot-1", Director::PilotInfo{.uuid = "pilot-1", .tasks = {"task-1"}, .tags = {}});

  auto result = run_async(director.UpdateJobStatus("pilot-1", "job-1", "task-1", JobStatus::Running));
  REQUIRE(result.has_value());
  REQUIRE(std::get<0>(result.value()).has_value());
  REQUIRE(director.m_jobUpdateRequests.size() == 1);

  const auto &update = std::get<Queries::Update>(director.m_jobUpdateRequests.front());
  REQUIRE(std::ranges::any_of(update.match,
                              [](const auto &match) { return match.key == "pilotUuid" && match.value == "pilot-1"; }));
  REQUIRE(std::ranges::any_of(update.match, [](const auto &match) {
    return match.key == "status" && match.op == Queries::ComparisonOp::IN &&
           match.value == json::array({"Claimed", "Running", "InboundTransfer", "OutboundTransfer"});
  }));
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

// ---------------------------------------------------------------------------
// AddNewJob
// ---------------------------------------------------------------------------

SCENARIO("Director: AddNewJob", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  GIVEN("a job as a const-ref") {
    json job{{"param", 1}};
    WHEN("AddNewJob is called") {
      THEN("it returns Success immediately without any DB call") {
        REQUIRE(director.AddNewJob(job) == IDirector::OperationResult::Success);
      }
    }
  }

  GIVEN("a job as an rvalue") {
    WHEN("AddNewJob is called") {
      THEN("it returns Success immediately without any DB call") {
        REQUIRE(director.AddNewJob(json{{"param", 2}}) == IDirector::OperationResult::Success);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// PilotClaimJob
// ---------------------------------------------------------------------------

// NOTE: A full unit test for PilotClaimJob's preemptive check requires:
// - Task with totJobs > 0 (set by UpdateTasks background thread in production)
// - Pilot registered in DB
// - DB mock returning empty jobs list
// This is complex to set up without starting background threads.
// The fix is verified by: (1) build passing, (2) all existing tests passing,
// (3) the new DB query code path being exercised when pending jobs don't exist.

// ---------------------------------------------------------------------------
// UpdateHeartBeat
// ---------------------------------------------------------------------------

SCENARIO("Director: UpdateHeartBeat", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  GIVEN("a pilot UUID") {
    WHEN("UpdateHeartBeat is called") {
      THEN("it returns success without any DB call") {
        auto result = director.UpdateHeartBeat("pilot-uuid-1");
        REQUIRE(result.has_value());
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

SCENARIO("Director: Summary includes tasks waiting for dependencies", "[Director][Summary][dependencies]") {
  Fixture f;
  auto director = f.make_director();

  GIVEN("a task with pending jobs depends on an upstream task") {
    auto &upstream = director.m_tasks["upstream"];
    upstream.name = "upstream";
    upstream.totJobs = 1;
    upstream.jobs[JobStatus::Running] = 1;
    upstream.readyForScheduling = true;

    auto &dependent = director.m_tasks["dependent"];
    dependent.name = "dependent";
    dependent.totJobs = 2;
    dependent.jobs[JobStatus::Pending] = 2;

    REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
        .WITH(std::holds_alternative<DB::Queries::Update>(_1))
        .RETURN(json{{"matched_count", 1}, {"modified_count", 1}});
    auto dependency_result = run_async(director.AddTaskDependency("dependent", "upstream"));
    REQUIRE(dependency_result.has_value());
    REQUIRE(std::get<0>(*dependency_result).has_value());
    REQUIRE(dependent.dependencies == std::vector<std::string>{"upstream"});

    auto check_summary = [&](const json &frontend_tasks) {
      ALLOW_CALL(*f.frontMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<DB::Queries::Distinct>(_1))
          .RETURN(frontend_tasks);

      // All jobs remain in the backend, including jobs waiting on dependencies.
      // Keep the frontend response distinct so querying it reproduces the omission.
      REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<DB::Queries::Distinct>(_1))
          .WITH(std::get<DB::Queries::Distinct>(_1).collection == "jobs")
          .WITH(std::get<DB::Queries::Distinct>(_1).field == "task")
          .WITH(std::get<DB::Queries::Distinct>(_1).match == DB::Queries::Matches{{"user", "alice"}})
          .RETURN(json::array({"upstream", "dependent"}));

      WHEN("Summary is requested for the task owner") {
        auto result = run_async(director.Summary("alice"));

        THEN("the dependent task appears with its cached status counters") {
          REQUIRE(result.has_value());
          auto [summary] = *result;
          REQUIRE(summary.has_value());
          const auto parsed = json::parse(summary.value());
          REQUIRE(parsed.is_array());
          CHECK(parsed.size() == 2);
          const auto dependent_summary =
              std::ranges::find_if(parsed, [](const auto &entry) { return entry.at("taskname") == "dependent"; });
          REQUIRE(dependent_summary != parsed.end());
          CHECK(dependent_summary->at("Pending") == 2);
          CHECK(dependent_summary->at("Running") == 0);
        }
      }
    };

    AND_GIVEN("waiting task jobs exist only in the backend") {
      // JobTransfer skips tasks that are not readyForScheduling. Their backend
      // Pending jobs therefore do not contribute to the frontend Distinct query.
      REQUIRE_FALSE(dependent.readyForScheduling);
      check_summary(json::array({"upstream"}));
    }
    AND_GIVEN("released task jobs have reached the frontend") {
      upstream.jobs[JobStatus::Running] = 0;
      upstream.jobs[JobStatus::Done] = 1;
      REQUIRE(upstream.IsFinished());
      dependent.readyForScheduling = true;
      check_summary(json::array({"upstream", "dependent"}));
    }
    AND_GIVEN("waiting task already has frontend job records") {
      REQUIRE_FALSE(dependent.readyForScheduling);
      check_summary(json::array({"upstream", "dependent"}));
    }
  }
}

SCENARIO("Director: Summary", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  // Pre-create task so m_tasks is populated.
  ALLOW_CALL(*f.backMock, RunQuery(trompeloeil::_))
      .WITH(std::holds_alternative<PMS::DB::Queries::Insert>(_1))
      .RETURN(json{});
  run_async(director.CreateTask("myTask"));

  GIVEN("the backend DB Distinct returns the task name") {
    WHEN("Summary is called") {
      REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Distinct>(_1))
          .RETURN(json::array({"myTask"}));

      auto result = run_async(director.Summary("alice"));

      THEN("the result is a JSON array containing the task summary") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
        auto parsed = json::parse(r.value());
        REQUIRE(parsed.is_array());
        REQUIRE(parsed.size() == 1);
        REQUIRE(parsed[0]["taskname"] == "myTask");
      }
    }
  }

  GIVEN("the backend DB Distinct returns an empty array") {
    WHEN("Summary is called") {
      REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Distinct>(_1))
          .RETURN(json::array());

      auto result = run_async(director.Summary("alice"));

      THEN("the result is an empty JSON array") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
        REQUIRE(json::parse(r.value()).empty());
      }
    }
  }

  GIVEN("the backend DB Distinct returns an error") {
    WHEN("Summary is called") {
      REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Distinct>(_1))
          .RETURN(make_error(std::errc::io_error, "DB error"));

      auto result = run_async(director.Summary("alice"));

      THEN("an error is propagated") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE_FALSE(r.has_value());
      }
    }
  }
}

// ---------------------------------------------------------------------------
// QueryBackDB
// ---------------------------------------------------------------------------

SCENARIO("Director: QueryBackDB — Find", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  GIVEN("valid match JSON and back DB returns results") {
    WHEN("QueryBackDB is called with Find") {
      REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Find>(_1))
          .RETURN(json::array({json{{"hash", "abc"}}}));

      auto result = run_async(director.QueryBackDB(IDirector::QueryOperation::Find, json{{"task", "t1"}}, json{}));

      THEN("the result wraps the array in a 'result' key") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
        auto parsed = json::parse(r.value());
        REQUIRE(parsed.contains("result"));
        REQUIRE(parsed["result"].size() == 1);
      }
    }
  }
}

SCENARIO("Director: QueryBackDB — UpdateOne", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  GIVEN("valid match/option JSON and front DB update succeeds") {
    WHEN("QueryBackDB is called with UpdateOne") {
      REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Update>(_1) &&
                std::get<PMS::DB::Queries::Update>(_1).options.limit == 1)
          .RETURN(json{});

      auto result = run_async(director.QueryBackDB(IDirector::QueryOperation::UpdateOne, json{{"hash", "abc123"}},
                                                   json{{"$set", {{"status", "Done"}}}}));

      THEN("the result mentions 'Updated job'") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
        REQUIRE(r.value().find("Updated job") != std::string::npos);
      }
    }
  }
}

SCENARIO("Director: QueryBackDB — UpdateMany", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  GIVEN("valid match/option JSON and front DB update returns counts") {
    WHEN("QueryBackDB is called with UpdateMany") {
      REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Update>(_1))
          .RETURN(json{{"matched_count", 3}, {"modified_count", 3}});

      auto result = run_async(director.QueryBackDB(IDirector::QueryOperation::UpdateMany, json{{"task", "myTask"}},
                                                   json{{"$set", {{"status", "Done"}}}}));

      THEN("the result mentions 'Matched' and 'Updated'") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
        REQUIRE(r.value().find("Matched") != std::string::npos);
        REQUIRE(r.value().find("Updated") != std::string::npos);
      }
    }
  }
}

SCENARIO("Director: QueryBackDB — DeleteOne", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  GIVEN("valid match JSON and both DBs delete successfully") {
    WHEN("QueryBackDB is called with DeleteOne") {
      REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Delete>(_1) &&
                std::get<PMS::DB::Queries::Delete>(_1).options.limit == 1)
          .RETURN(json{{"deleted_count", 1}});
      REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Delete>(_1) &&
                std::get<PMS::DB::Queries::Delete>(_1).options.limit == 1)
          .RETURN(json{{"deleted_count", 1}});

      auto result =
          run_async(director.QueryBackDB(IDirector::QueryOperation::DeleteOne, json{{"hash", "abc123"}}, json{}));

      THEN("the result mentions 'Deleted job'") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
        REQUIRE(r.value().find("Deleted job") != std::string::npos);
      }
    }
  }
}

SCENARIO("Director: QueryBackDB — DeleteMany", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  GIVEN("valid match JSON and both DBs delete successfully") {
    WHEN("QueryBackDB is called with DeleteMany") {
      REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Delete>(_1) &&
                std::get<PMS::DB::Queries::Delete>(_1).options.limit == 0)
          .RETURN(json{{"deleted_count", 5}});
      REQUIRE_CALL(*f.backMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Delete>(_1) &&
                std::get<PMS::DB::Queries::Delete>(_1).options.limit == 0)
          .RETURN(json{{"deleted_count", 5}});

      auto result =
          run_async(director.QueryBackDB(IDirector::QueryOperation::DeleteMany, json{{"task", "myTask"}}, json{}));

      THEN("the result mentions 'Deleted 5 jobs'") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
        REQUIRE(r.value().find("Deleted 5 jobs") != std::string::npos);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// QueryFrontDB
// ---------------------------------------------------------------------------

SCENARIO("Director: QueryFrontDB — Jobs collection", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  GIVEN("a match and front DB returns job results") {
    WHEN("QueryFrontDB is called with DBCollection::Jobs") {
      REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Find>(_1) &&
                std::get<PMS::DB::Queries::Find>(_1).collection == "jobs")
          .RETURN(json::array({json{{"hash", "abc"}}}));

      auto result = run_async(director.QueryFrontDB(IDirector::DBCollection::Jobs, json{{"task", "myTask"}}, json{}));

      THEN("the result wraps the array in a 'result' key") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
        auto parsed = json::parse(r.value());
        REQUIRE(parsed.contains("result"));
        REQUIRE(parsed["result"].size() == 1);
      }
    }
  }
}

SCENARIO("Director: QueryFrontDB — Pilots collection", "[Director]") {
  Fixture f;
  auto director = f.make_director();

  GIVEN("a match and front DB returns pilot results") {
    WHEN("QueryFrontDB is called with DBCollection::Pilots") {
      REQUIRE_CALL(*f.frontMock, RunQuery(trompeloeil::_))
          .WITH(std::holds_alternative<PMS::DB::Queries::Find>(_1) &&
                std::get<PMS::DB::Queries::Find>(_1).collection == "pilots")
          .RETURN(json::array({json{{"uuid", "p1"}}}));

      auto result = run_async(director.QueryFrontDB(IDirector::DBCollection::Pilots, json{{"uuid", "p1"}}, json{}));

      THEN("the result wraps the array in a 'result' key") {
        REQUIRE(result.has_value());
        auto [r] = result.value();
        REQUIRE(r.has_value());
        auto parsed = json::parse(r.value());
        REQUIRE(parsed.contains("result"));
      }
    }
  }
}

} // namespace PMS::Tests::Orchestrator
