// c++ headers
#include <memory>
#include <string>
#include <unordered_map>

// external dependencies
#include <catch2/catch_test_macros.hpp>
#include <exec/static_thread_pool.hpp>
#include <spdlog/fmt/bundled/format.h>
#include <stdexec/execution.hpp>

// our headers
#include "common/Job.h"
#include "orchestrator/Commands.h"
#include "orchestrator/IDirector.h"
#include "orchestrator/Server.h"

// from https://github.com/okdshin/PicoSHA2
#include "orchestrator/picosha2.h"

using json = nlohmann::json;
using namespace PMS;
using namespace PMS::Orchestrator;

namespace PMS::Tests::Orchestrator {

// ---------------------------------------------------------------------------
// MockDirector — hand-crafted stub with settable return values
// ---------------------------------------------------------------------------

class MockDirector : public IDirector {
public:
  // --- token validation: map task → expected token ---
  std::unordered_map<std::string, std::string> valid_tokens;

  OperationResult ValidateTaskToken(std::string_view task, std::string_view token) const override {
    auto it = valid_tokens.find(std::string{task});
    if (it == valid_tokens.end())
      return OperationResult::DatabaseError;
    return it->second == token ? OperationResult::Success : OperationResult::ProcessError;
  }

  // --- configurable return values ---
  ErrorOr<std::string> create_task_result{std::string{"mock-token"}};
  ErrorOr<void> clear_task_result{};
  ErrorOr<void> add_task_dep_result{};
  ErrorOr<json> pilot_claim_job_result{json::object()};
  ErrorOr<void> update_job_status_result{};
  ErrorOr<NewPilotResult> register_pilot_result{
      NewPilotResult{OperationResult::Success, std::vector<std::string>{"t1"}, {}}};
  ErrorOr<void> delete_heartbeat_result{};
  ErrorOr<std::string> query_back_db_result{std::string{R"({"result":[]})"}};
  ErrorOr<std::string> query_front_db_result{std::string{R"({"result":[]})"}};
  ErrorOr<std::string> summary_result{std::string{"[]"}};
  ErrorOr<void> reset_failed_jobs_result{};
  OperationResult add_new_job_result = OperationResult::Success;
  ErrorOr<void> update_heartbeat_result{};

  // --- IDirector implementations ---
  OperationResult AddNewJob(const json &) override { return add_new_job_result; }
  OperationResult AddNewJob(json &&) override { return add_new_job_result; }

  Async<ErrorOr<json>> PilotClaimJob(std::string_view) override { co_return pilot_claim_job_result; }

  Async<ErrorOr<void>> UpdateJobStatus(std::string_view, std::string_view, std::string_view, JobStatus) override {
    co_return update_job_status_result;
  }

  Async<ErrorOr<NewPilotResult>> RegisterNewPilot(std::string_view, std::string_view,
                                                  const std::vector<std::pair<std::string, std::string>> &,
                                                  const std::vector<std::string> &, const json &) override {
    co_return register_pilot_result;
  }

  ErrorOr<void> UpdateHeartBeat(std::string_view) override { return update_heartbeat_result; }

  Async<ErrorOr<void>> DeleteHeartBeat(std::string_view) override { co_return delete_heartbeat_result; }

  Async<ErrorOr<void>> AddTaskDependency(const std::string &, const std::string &) override {
    co_return add_task_dep_result;
  }

  Async<ErrorOr<std::string>> CreateTask(const std::string &) override { co_return create_task_result; }

  Async<ErrorOr<void>> ClearTask(const std::string &, bool) override { co_return clear_task_result; }

  Async<ErrorOr<std::string>> Summary(const std::string &) override { co_return summary_result; }

  Async<ErrorOr<std::string>> QueryBackDB(QueryOperation, const json &, const json &) override {
    co_return query_back_db_result;
  }

  Async<ErrorOr<std::string>> QueryFrontDB(DBCollection, const json &, const json &) override {
    co_return query_front_db_result;
  }

  Async<ErrorOr<void>> ResetFailedJobs(std::string_view) override { co_return reset_failed_jobs_result; }
};

// ---------------------------------------------------------------------------
// ServerProxy — exposes protected Server methods for testing
// ---------------------------------------------------------------------------

class ServerProxy : public Server {
public:
  explicit ServerProxy(std::shared_ptr<IDirector> d) : Server(std::move(d)) {}
  using Server::HandleCommand;
  using Server::ValidateTaskToken;
};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

static exec::static_thread_pool g_pool{2};

static std::string run_user(ServerProxy &server, UserCommand cmd) {
  auto result = stdexec::sync_wait(stdexec::on(g_pool.get_scheduler(), server.HandleCommand(std::move(cmd))));
  REQUIRE(result.has_value());
  auto [r] = result.value();
  return r;
}

static std::string run_pilot(ServerProxy &server, PilotCommand cmd) {
  auto result = stdexec::sync_wait(stdexec::on(g_pool.get_scheduler(), server.HandleCommand(std::move(cmd))));
  REQUIRE(result.has_value());
  auto [r] = result.value();
  return r;
}

// ---------------------------------------------------------------------------
// UserCommand scenarios
// ---------------------------------------------------------------------------

SCENARIO("HandleCommand: LivenessProbe", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  ServerProxy server{mock};

  GIVEN("a livenessProbe command") {
    WHEN("HandleCommand is called") {
      auto reply = run_user(server, OrchCommand<LivenessProbe>{});
      THEN("the reply is 'OK'") { REQUIRE(reply == "OK"); }
    }
  }
}

SCENARIO("HandleCommand: CreateTask", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  ServerProxy server{mock};

  GIVEN("the director returns a token successfully") {
    mock->create_task_result = std::string{"generated-token"};
    WHEN("HandleCommand is called with createTask") {
      auto reply = run_user(server, OrchCommand<CreateTask>{{.task = "myTask"}});
      THEN("the reply contains the task name and token") {
        REQUIRE(reply.find("myTask") != std::string::npos);
        REQUIRE(reply.find("generated-token") != std::string::npos);
      }
    }
  }

  GIVEN("the director returns an error") {
    mock->create_task_result = make_error(std::errc::file_exists, "Task already exists");
    WHEN("HandleCommand is called with createTask") {
      auto reply = run_user(server, OrchCommand<CreateTask>{{.task = "myTask"}});
      THEN("the reply indicates failure") { REQUIRE(reply.find("Failed to create task") != std::string::npos); }
    }
  }
}

SCENARIO("HandleCommand: ClearTask", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  mock->valid_tokens["myTask"] = "valid-token";
  ServerProxy server{mock};

  GIVEN("a valid token and successful director call") {
    WHEN("HandleCommand is called with clearTask") {
      auto reply = run_user(server, OrchCommand<ClearTask>{{.task = "myTask", .token = "valid-token"}});
      THEN("the reply confirms task cleared") {
        REQUIRE(reply.find("cleared") != std::string::npos);
        REQUIRE(reply.find("myTask") != std::string::npos);
      }
    }
  }

  GIVEN("an invalid token") {
    WHEN("HandleCommand is called with clearTask") {
      auto reply = run_user(server, OrchCommand<ClearTask>{{.task = "myTask", .token = "bad-token"}});
      THEN("the reply indicates invalid token") { REQUIRE(reply.find("myTask") != std::string::npos); }
    }
  }
}

SCENARIO("HandleCommand: CleanTask", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  mock->valid_tokens["myTask"] = "valid-token";
  ServerProxy server{mock};

  GIVEN("a valid token and successful director call") {
    WHEN("HandleCommand is called with cleanTask") {
      auto reply = run_user(server, OrchCommand<CleanTask>{{.task = "myTask", .token = "valid-token"}});
      THEN("the reply confirms task cleaned") {
        REQUIRE(reply.find("cleaned") != std::string::npos);
        REQUIRE(reply.find("myTask") != std::string::npos);
      }
    }
  }
}

SCENARIO("HandleCommand: DeclareTaskDependency", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  mock->valid_tokens["A"] = "tok-a";
  ServerProxy server{mock};

  GIVEN("a valid token and successful dependency creation") {
    WHEN("HandleCommand is called") {
      auto reply =
          run_user(server, OrchCommand<DeclareTaskDependency>{{.task = "A", .dependsOn = "B", .token = "tok-a"}});
      THEN("the reply mentions both tasks") {
        REQUIRE(reply.find("A") != std::string::npos);
        REQUIRE(reply.find("B") != std::string::npos);
      }
    }
  }
}

SCENARIO("HandleCommand: CheckTaskToken", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  mock->valid_tokens["myTask"] = "right-token";
  ServerProxy server{mock};

  GIVEN("the correct token") {
    WHEN("HandleCommand is called") {
      auto reply = run_user(server, OrchCommand<CheckTaskToken>{{.task = "myTask", .token = "right-token"}});
      THEN("the reply confirms validity") { REQUIRE(reply.find("pair is valid") != std::string::npos); }
    }
  }

  GIVEN("a wrong token") {
    WHEN("HandleCommand is called") {
      auto reply = run_user(server, OrchCommand<CheckTaskToken>{{.task = "myTask", .token = "wrong"}});
      THEN("the reply indicates invalid token") { REQUIRE(reply.find("Invalid token") != std::string::npos); }
    }
  }
}

SCENARIO("HandleCommand: SubmitJob", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  mock->valid_tokens["myTask"] = "valid-token";
  ServerProxy server{mock};

  GIVEN("a valid token and AddNewJob returns Success") {
    json job = {{"param", 42}};
    WHEN("HandleCommand is called with submitJob") {
      // Compute expected hash the same way Server does.
      json expected_job = job;
      expected_job["task"] = "myTask";
      std::string expected_hash;
      picosha2::hash256_hex_string(expected_job.dump(), expected_hash);

      auto reply = run_user(server, OrchCommand<SubmitJob>{{.job = job, .task = "myTask", .token = "valid-token"}});

      THEN("the reply contains the computed hash") {
        REQUIRE(reply.find("Job received") != std::string::npos);
        REQUIRE(reply.find(expected_hash) != std::string::npos);
      }
    }
  }

  GIVEN("a valid token but AddNewJob fails") {
    mock->add_new_job_result = IDirector::OperationResult::DatabaseError;
    auto reply = run_user(server, OrchCommand<SubmitJob>{{.job = json{}, .task = "myTask", .token = "valid-token"}});
    THEN("the reply indicates failure") { REQUIRE(reply.find("failed") != std::string::npos); }
  }
}

SCENARIO("HandleCommand: FindJobs", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  mock->query_back_db_result = std::string{R"({"result":[{"hash":"abc"}]})"};
  ServerProxy server{mock};

  GIVEN("the DB query succeeds") {
    WHEN("HandleCommand is called with findJobs") {
      auto reply = run_user(server, OrchCommand<FindJobs>{{.match = json{}, .filter = json{}}});
      THEN("the reply contains the query result") { REQUIRE(reply.find("result") != std::string::npos); }
    }
  }
}

SCENARIO("HandleCommand: ResetJobs", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  mock->query_back_db_result = std::string{R"(Matched 5 jobs. Updated 5 jobs)"};
  ServerProxy server{mock};

  GIVEN("the DB update succeeds") {
    WHEN("HandleCommand is called with resetJobs") {
      auto reply = run_user(server, OrchCommand<ResetJobs>{{.match = json{}}});
      THEN("the reply reflects the update") { REQUIRE_FALSE(reply.empty()); }
    }
  }
}

SCENARIO("HandleCommand: FindPilots", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  mock->query_front_db_result = std::string{R"({"result":[{"uuid":"p1"}]})"};
  ServerProxy server{mock};

  GIVEN("the DB query succeeds") {
    WHEN("HandleCommand is called with findPilots") {
      auto reply = run_user(server, OrchCommand<FindPilots>{{.match = json{}, .filter = json{}}});
      THEN("the reply contains pilot data") { REQUIRE(reply.find("result") != std::string::npos); }
    }
  }
}

SCENARIO("HandleCommand: Summary", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  mock->summary_result = std::string{R"([{"taskname":"t1"}])"};
  ServerProxy server{mock};

  GIVEN("the director returns a summary") {
    WHEN("HandleCommand is called with summary") {
      auto reply = run_user(server, OrchCommand<Summary>{{.user = "alice"}});
      THEN("the reply contains task information") { REQUIRE(reply.find("taskname") != std::string::npos); }
    }
  }
}

SCENARIO("HandleCommand: ResetFailedJobs", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  mock->valid_tokens["myTask"] = "valid-token";
  ServerProxy server{mock};

  GIVEN("a valid token and successful director call") {
    WHEN("HandleCommand is called") {
      auto reply = run_user(server, OrchCommand<ResetFailedJobs>{{.task = "myTask", .token = "valid-token"}});
      THEN("the reply confirms jobs were reset") { REQUIRE(reply.find("reset") != std::string::npos); }
    }
  }

  GIVEN("an invalid token") {
    WHEN("HandleCommand is called") {
      auto reply = run_user(server, OrchCommand<ResetFailedJobs>{{.task = "myTask", .token = "bad"}});
      THEN("the reply indicates invalid token") {
        REQUIRE_FALSE(reply.find("reset") != std::string::npos);
        REQUIRE(reply.find("myTask") != std::string::npos);
      }
    }
  }
}

SCENARIO("HandleCommand: InvalidCommand (user)", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  ServerProxy server{mock};

  GIVEN("an invalid user command with an error message") {
    auto reply = run_user(server, OrchCommand<InvalidCommand>{{.errorMessage = "bad input"}});
    THEN("the error message is echoed back") { REQUIRE(reply == "bad input"); }
  }
}

// ---------------------------------------------------------------------------
// PilotCommand scenarios
// ---------------------------------------------------------------------------

SCENARIO("HandleCommand: ClaimJob", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  mock->pilot_claim_job_result = json{{"hash", "job-hash-1"}};
  ServerProxy server{mock};

  GIVEN("a pilot claims a job successfully") {
    WHEN("HandleCommand is called with p_claimJob") {
      auto reply = run_pilot(server, OrchCommand<ClaimJob>{{.uuid = "uuid-1"}});
      THEN("the reply is the job JSON dump") { REQUIRE(reply.find("job-hash-1") != std::string::npos); }
    }
  }
}

SCENARIO("HandleCommand: UpdateJobStatus", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  ServerProxy server{mock};

  GIVEN("the director returns success") {
    WHEN("HandleCommand is called with p_updateJobStatus") {
      auto reply = run_pilot(server, OrchCommand<UpdateJobStatus>{
                                         {.status = JobStatus::Done, .uuid = "uuid-1", .hash = "h1", .task = "t1"}});
      THEN("the reply is 'Ok'") { REQUIRE(reply == "Ok"); }
    }
  }

  GIVEN("the director returns an error") {
    mock->update_job_status_result = make_error(std::errc::invalid_argument, "unknown pilot");
    WHEN("HandleCommand is called") {
      auto reply = run_pilot(server, OrchCommand<UpdateJobStatus>{
                                         {.status = JobStatus::Done, .uuid = "bad-uuid", .hash = "h1", .task = "t1"}});
      THEN("the reply is the error message") { REQUIRE(reply.find("unknown pilot") != std::string::npos); }
    }
  }
}

SCENARIO("HandleCommand: RegisterNewPilot", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  mock->register_pilot_result =
      IDirector::NewPilotResult{IDirector::OperationResult::Success, {"task-1", "task-2"}, {}};
  ServerProxy server{mock};

  GIVEN("the director returns a successful registration") {
    WHEN("HandleCommand is called with p_registerNewPilot") {
      auto reply = run_pilot(
          server,
          OrchCommand<RegisterNewPilot>{
              {.uuid = "uuid-1", .user = "alice", .tasks = {{"task-1", "tok1"}}, .tags = {}, .host_info = json{}}});
      THEN("the reply contains the valid tasks") {
        auto reply_json = json::parse(reply);
        REQUIRE(reply_json.contains("validTasks"));
        REQUIRE(reply_json["validTasks"].size() == 2);
      }
    }
  }

  GIVEN("the director returns a registration failure") {
    mock->register_pilot_result = make_error(std::errc::io_error, "DB error");
    WHEN("HandleCommand is called") {
      auto reply =
          run_pilot(server, OrchCommand<RegisterNewPilot>{
                                {.uuid = "uuid-2", .user = "alice", .tasks = {}, .tags = {}, .host_info = json{}}});
      THEN("the reply indicates failure") { REQUIRE(reply.find("Could not register pilot") != std::string::npos); }
    }
  }
}

SCENARIO("HandleCommand: UpdateHeartBeat", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  ServerProxy server{mock};

  GIVEN("the director heartbeat update succeeds") {
    WHEN("HandleCommand is called with p_updateHeartBeat") {
      auto reply = run_pilot(server, OrchCommand<UpdateHeartBeat>{{.uuid = "uuid-1"}});
      THEN("the reply is 'Ok'") { REQUIRE(reply == "Ok"); }
    }
  }

  GIVEN("the director heartbeat update fails") {
    mock->update_heartbeat_result = make_error(std::errc::no_such_process, "unknown pilot");
    WHEN("HandleCommand is called") {
      auto reply = run_pilot(server, OrchCommand<UpdateHeartBeat>{{.uuid = "uuid-bad"}});
      THEN("the reply indicates failure") { REQUIRE(reply.find("Failed") != std::string::npos); }
    }
  }
}

SCENARIO("HandleCommand: DeleteHeartBeat", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  ServerProxy server{mock};

  GIVEN("the director delete succeeds") {
    WHEN("HandleCommand is called with p_deleteHeartBeat") {
      auto reply = run_pilot(server, OrchCommand<DeleteHeartBeat>{{.uuid = "uuid-1"}});
      THEN("the reply is 'Ok'") { REQUIRE(reply == "Ok"); }
    }
  }
}

SCENARIO("HandleCommand: InvalidCommand (pilot)", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  ServerProxy server{mock};

  GIVEN("an invalid pilot command") {
    auto reply = run_pilot(server, OrchCommand<InvalidCommand>{{.errorMessage = "bad pilot cmd"}});
    THEN("the error message is echoed back") { REQUIRE(reply == "bad pilot cmd"); }
  }
}

SCENARIO("HandleCommand: Test", "[Server][HandleCommand]") {
  auto mock = std::make_shared<MockDirector>();
  ServerProxy server{mock};

  GIVEN("a stress-test command") {
    auto reply = run_pilot(server, OrchCommand<Test>{});
    THEN("the reply is 'ok'") { REQUIRE(reply == "ok"); }
  }
}

} // namespace PMS::Tests::Orchestrator
