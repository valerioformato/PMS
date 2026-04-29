#include <catch2/catch_test_macros.hpp>
#include <nlohmann/json.hpp>

#include "orchestrator/CommandParser.h"
#include "orchestrator/Commands.h"

using json = nlohmann::json;
using namespace PMS::Orchestrator;
using namespace PMS::Orchestrator::CommandParser;

namespace PMS::Tests::Orchestrator {

// ---------------------------------------------------------------------------
// toUserCommand
// ---------------------------------------------------------------------------

SCENARIO("toUserCommand: malformed or missing command field", "[Server][CommandParsing]") {
  GIVEN("A JSON object with no 'command' field and no 'livenessProbe'") {
    auto msg = json::parse(R"({"foo": "bar"})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("An InvalidCommand is returned") { REQUIRE(std::holds_alternative<OrchCommand<InvalidCommand>>(result)); }
    }
  }

  GIVEN("A livenessProbe message") {
    auto msg = json::parse(R"({"livenessProbe": true})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("A LivenessProbe command is returned") {
        REQUIRE(std::holds_alternative<OrchCommand<LivenessProbe>>(result));
      }
    }
  }

  GIVEN("An unknown command name") {
    auto msg = json::parse(R"({"command": "doSomethingUnknown"})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("An InvalidCommand is returned") { REQUIRE(std::holds_alternative<OrchCommand<InvalidCommand>>(result)); }
    }
  }
}

SCENARIO("toUserCommand: valid user commands", "[Server][CommandParsing]") {
  GIVEN("A valid createTask message") {
    auto msg = json::parse(R"({"command": "createTask", "task": "myTask"})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("A CreateTask command is returned with the correct task name") {
        REQUIRE(std::holds_alternative<OrchCommand<CreateTask>>(result));
        REQUIRE(std::get<OrchCommand<CreateTask>>(result).cmd.task == "myTask");
      }
    }
  }

  GIVEN("A createTask message missing the 'task' field") {
    auto msg = json::parse(R"({"command": "createTask"})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("An InvalidCommand is returned") { REQUIRE(std::holds_alternative<OrchCommand<InvalidCommand>>(result)); }
    }
  }

  GIVEN("A valid clearTask message") {
    auto msg = json::parse(R"({"command": "clearTask", "task": "myTask", "token": "myToken"})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("A ClearTask command is returned with correct task and token") {
        REQUIRE(std::holds_alternative<OrchCommand<ClearTask>>(result));
        auto &cmd = std::get<OrchCommand<ClearTask>>(result).cmd;
        REQUIRE(cmd.task == "myTask");
        REQUIRE(cmd.token == "myToken");
      }
    }
  }

  GIVEN("A valid cleanTask message") {
    auto msg = json::parse(R"({"command": "cleanTask", "task": "myTask", "token": "myToken"})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("A CleanTask command is returned") { REQUIRE(std::holds_alternative<OrchCommand<CleanTask>>(result)); }
    }
  }

  GIVEN("A valid submitJob message") {
    auto msg = json::parse(R"({"command": "submitJob", "job": {}, "task": "myTask", "token": "myToken"})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("A SubmitJob command is returned") { REQUIRE(std::holds_alternative<OrchCommand<SubmitJob>>(result)); }
    }
  }

  GIVEN("A valid findJobs message") {
    auto msg = json::parse(R"({"command": "findJobs", "match": {}})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("A FindJobs command is returned") { REQUIRE(std::holds_alternative<OrchCommand<FindJobs>>(result)); }
    }
  }

  GIVEN("A valid resetJobs message") {
    auto msg = json::parse(R"({"command": "resetJobs", "match": {}})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("A ResetJobs command is returned") { REQUIRE(std::holds_alternative<OrchCommand<ResetJobs>>(result)); }
    }
  }

  GIVEN("A valid findPilots message") {
    auto msg = json::parse(R"({"command": "findPilots", "match": {}})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("A FindPilots command is returned") { REQUIRE(std::holds_alternative<OrchCommand<FindPilots>>(result)); }
    }
  }

  GIVEN("A valid summary message") {
    auto msg = json::parse(R"({"command": "summary", "user": "alice"})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("A Summary command is returned with the correct user") {
        REQUIRE(std::holds_alternative<OrchCommand<Summary>>(result));
        REQUIRE(std::get<OrchCommand<Summary>>(result).cmd.user == "alice");
      }
    }
  }

  GIVEN("A valid declareTaskDependency message") {
    auto msg =
        json::parse(R"({"command": "declareTaskDependency", "task": "A", "dependsOn": "B", "token": "myToken"})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("A DeclareTaskDependency command is returned with correct fields") {
        REQUIRE(std::holds_alternative<OrchCommand<DeclareTaskDependency>>(result));
        auto &cmd = std::get<OrchCommand<DeclareTaskDependency>>(result).cmd;
        REQUIRE(cmd.task == "A");
        REQUIRE(cmd.dependsOn == "B");
        REQUIRE(cmd.token == "myToken");
      }
    }
  }

  GIVEN("A valid validateTaskToken message") {
    auto msg = json::parse(R"({"command": "validateTaskToken", "task": "myTask", "token": "myToken"})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("A CheckTaskToken command is returned") {
        REQUIRE(std::holds_alternative<OrchCommand<CheckTaskToken>>(result));
      }
    }
  }

  GIVEN("A valid resetFailedJobs message") {
    auto msg = json::parse(R"({"command": "resetFailedJobs", "task": "myTask", "token": "myToken"})");
    WHEN("toUserCommand is called") {
      auto result = toUserCommand(msg);
      THEN("A ResetFailedJobs command is returned") {
        REQUIRE(std::holds_alternative<OrchCommand<ResetFailedJobs>>(result));
      }
    }
  }
}

// ---------------------------------------------------------------------------
// toPilotCommand
// ---------------------------------------------------------------------------

SCENARIO("toPilotCommand: malformed or missing command field", "[Server][CommandParsing]") {
  GIVEN("A non-object JSON value") {
    auto msg = json::parse(R"("notanobject")");
    WHEN("toPilotCommand is called") {
      auto result = toPilotCommand(msg);
      THEN("An InvalidCommand is returned") { REQUIRE(std::holds_alternative<OrchCommand<InvalidCommand>>(result)); }
    }
  }

  GIVEN("A JSON object missing the 'command' field") {
    auto msg = json::parse(R"({"foo": "bar"})");
    WHEN("toPilotCommand is called") {
      auto result = toPilotCommand(msg);
      THEN("An InvalidCommand is returned") { REQUIRE(std::holds_alternative<OrchCommand<InvalidCommand>>(result)); }
    }
  }

  GIVEN("A JSON object with an unknown command") {
    auto msg = json::parse(R"({"command": "p_unknown"})");
    WHEN("toPilotCommand is called") {
      auto result = toPilotCommand(msg);
      THEN("An InvalidCommand is returned") { REQUIRE(std::holds_alternative<OrchCommand<InvalidCommand>>(result)); }
    }
  }
}

SCENARIO("toPilotCommand: valid pilot commands", "[Server][CommandParsing]") {
  GIVEN("A valid p_claimJob message") {
    auto msg = json::parse(R"({"command": "p_claimJob", "pilotUuid": "uuid-1"})");
    WHEN("toPilotCommand is called") {
      auto result = toPilotCommand(msg);
      THEN("A ClaimJob command is returned with the correct uuid") {
        REQUIRE(std::holds_alternative<OrchCommand<ClaimJob>>(result));
        REQUIRE(std::get<OrchCommand<ClaimJob>>(result).cmd.uuid == "uuid-1");
      }
    }
  }

  GIVEN("A valid p_updateJobStatus message") {
    auto msg = json::parse(
        R"({"command": "p_updateJobStatus", "pilotUuid": "uuid-1", "status": "Done", "hash": "abc", "task": "t1"})");
    WHEN("toPilotCommand is called") {
      auto result = toPilotCommand(msg);
      THEN("An UpdateJobStatus command is returned") {
        REQUIRE(std::holds_alternative<OrchCommand<UpdateJobStatus>>(result));
      }
    }
  }

  GIVEN("A p_updateJobStatus message with an invalid status string") {
    auto msg = json::parse(
        R"({"command": "p_updateJobStatus", "pilotUuid": "uuid-1", "status": "BOGUS", "hash": "abc", "task": "t1"})");
    WHEN("toPilotCommand is called") {
      auto result = toPilotCommand(msg);
      THEN("An InvalidCommand is returned") { REQUIRE(std::holds_alternative<OrchCommand<InvalidCommand>>(result)); }
    }
  }

  GIVEN("A valid p_registerNewPilot message") {
    auto msg = json::parse(
        R"({"command": "p_registerNewPilot", "pilotUuid": "uuid-1", "user": "alice", "tasks": [{"name":"t1","token":"tok1"}], "host": {}})");
    WHEN("toPilotCommand is called") {
      auto result = toPilotCommand(msg);
      THEN("A RegisterNewPilot command is returned with correct fields") {
        REQUIRE(std::holds_alternative<OrchCommand<RegisterNewPilot>>(result));
        auto &cmd = std::get<OrchCommand<RegisterNewPilot>>(result).cmd;
        REQUIRE(cmd.uuid == "uuid-1");
        REQUIRE(cmd.user == "alice");
        REQUIRE(cmd.tasks.size() == 1);
        REQUIRE(cmd.tasks[0].first == "t1");
        REQUIRE(cmd.tasks[0].second == "tok1");
      }
    }
  }

  GIVEN("A valid p_updateHeartBeat message") {
    auto msg = json::parse(R"({"command": "p_updateHeartBeat", "uuid": "uuid-1"})");
    WHEN("toPilotCommand is called") {
      auto result = toPilotCommand(msg);
      THEN("An UpdateHeartBeat command is returned") {
        REQUIRE(std::holds_alternative<OrchCommand<UpdateHeartBeat>>(result));
      }
    }
  }

  GIVEN("A valid p_deleteHeartBeat message") {
    auto msg = json::parse(R"({"command": "p_deleteHeartBeat", "uuid": "uuid-1"})");
    WHEN("toPilotCommand is called") {
      auto result = toPilotCommand(msg);
      THEN("A DeleteHeartBeat command is returned") {
        REQUIRE(std::holds_alternative<OrchCommand<DeleteHeartBeat>>(result));
      }
    }
  }

  GIVEN("A valid p_test message") {
    auto msg = json::parse(R"({"command": "p_test"})");
    WHEN("toPilotCommand is called") {
      auto result = toPilotCommand(msg);
      THEN("A Test command is returned") { REQUIRE(std::holds_alternative<OrchCommand<Test>>(result)); }
    }
  }
}

} // namespace PMS::Tests::Orchestrator
