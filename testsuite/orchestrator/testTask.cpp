#include <catch2/catch_test_macros.hpp>

#include "common/Job.h"
#include "orchestrator/Task.h"

using namespace PMS::Orchestrator;
using PMS::JobStatus;

namespace PMS::Tests::Orchestrator {

SCENARIO("Task::IsActive", "[Task]") {
  GIVEN("A zero-job task") {
    Task t{};
    THEN("IsActive returns false regardless of readyForScheduling") {
      REQUIRE_FALSE(t.IsActive());
      t.readyForScheduling = true;
      REQUIRE_FALSE(t.IsActive());
    }
  }

  GIVEN("A task with jobs but readyForScheduling = false") {
    Task t{};
    t.totJobs = 5;
    THEN("IsActive returns false") { REQUIRE_FALSE(t.IsActive()); }
  }

  GIVEN("A task with jobs, readyForScheduling = true, and no done/failed jobs") {
    Task t{};
    t.totJobs = 5;
    t.readyForScheduling = true;
    THEN("IsActive returns true") { REQUIRE(t.IsActive()); }
  }

  GIVEN("A task where all jobs are Done") {
    Task t{};
    t.totJobs = 5;
    t.readyForScheduling = true;
    t.jobs[JobStatus::Done] = 5;
    THEN("IsActive returns false") { REQUIRE_FALSE(t.IsActive()); }
  }

  GIVEN("A task where all jobs are Failed") {
    Task t{};
    t.totJobs = 5;
    t.readyForScheduling = true;
    t.jobs[JobStatus::Failed] = 5;
    THEN("IsActive returns false") { REQUIRE_FALSE(t.IsActive()); }
  }

  GIVEN("A task where done + failed equals totJobs") {
    Task t{};
    t.totJobs = 5;
    t.readyForScheduling = true;
    t.jobs[JobStatus::Done] = 3;
    t.jobs[JobStatus::Failed] = 2;
    THEN("IsActive returns false") { REQUIRE_FALSE(t.IsActive()); }
  }
}

SCENARIO("Task::IsFinished", "[Task]") {
  GIVEN("A zero-job task") {
    Task t{};
    THEN("IsFinished returns false") { REQUIRE_FALSE(t.IsFinished()); }
  }

  GIVEN("A task where all jobs are Done") {
    Task t{};
    t.totJobs = 5;
    t.jobs[JobStatus::Done] = 5;
    THEN("IsFinished returns true") { REQUIRE(t.IsFinished()); }
  }

  GIVEN("A task with only some jobs Done") {
    Task t{};
    t.totJobs = 5;
    t.jobs[JobStatus::Done] = 3;
    THEN("IsFinished returns false") { REQUIRE_FALSE(t.IsFinished()); }
  }
}

SCENARIO("Task::IsExhausted", "[Task]") {
  GIVEN("A zero-job task") {
    Task t{};
    THEN("IsExhausted returns false") { REQUIRE_FALSE(t.IsExhausted()); }
  }

  GIVEN("A task with no Pending and no Error jobs") {
    Task t{};
    t.totJobs = 5;
    t.jobs[JobStatus::Done] = 5;
    THEN("IsExhausted returns true") { REQUIRE(t.IsExhausted()); }
  }

  GIVEN("A task with Pending jobs remaining") {
    Task t{};
    t.totJobs = 5;
    t.jobs[JobStatus::Pending] = 2;
    t.jobs[JobStatus::Done] = 3;
    THEN("IsExhausted returns false") { REQUIRE_FALSE(t.IsExhausted()); }
  }

  GIVEN("A task with Error jobs remaining") {
    Task t{};
    t.totJobs = 5;
    t.jobs[JobStatus::Error] = 1;
    t.jobs[JobStatus::Done] = 4;
    THEN("IsExhausted returns false") { REQUIRE_FALSE(t.IsExhausted()); }
  }

  // IsExhausted only checks Pending + Error == 0; Running/Claimed jobs don't count
  GIVEN("A task with only Running jobs (no Pending, no Error)") {
    Task t{};
    t.totJobs = 5;
    t.jobs[JobStatus::Running] = 5;
    THEN("IsExhausted returns true (Running jobs are not counted)") { REQUIRE(t.IsExhausted()); }
  }
}

SCENARIO("Task::IsFailed", "[Task]") {
  GIVEN("A task that is exhausted with Failed jobs") {
    Task t{};
    t.totJobs = 5;
    t.jobs[JobStatus::Done] = 3;
    t.jobs[JobStatus::Failed] = 2;
    THEN("IsFailed returns true") { REQUIRE(t.IsFailed()); }
  }

  GIVEN("A task that is exhausted with no Failed jobs") {
    Task t{};
    t.totJobs = 5;
    t.jobs[JobStatus::Done] = 5;
    THEN("IsFailed returns false") { REQUIRE_FALSE(t.IsFailed()); }
  }

  GIVEN("A task that is not exhausted") {
    Task t{};
    t.totJobs = 5;
    t.jobs[JobStatus::Pending] = 2;
    t.jobs[JobStatus::Failed] = 3;
    THEN("IsFailed returns false even with Failed jobs") { REQUIRE_FALSE(t.IsFailed()); }
  }
}

} // namespace PMS::Tests::Orchestrator
