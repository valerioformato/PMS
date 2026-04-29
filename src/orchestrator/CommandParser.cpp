// c++ headers
#include <ranges>
#include <unordered_map>

// external dependencies
#include <magic_enum/magic_enum.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/fmt/bundled/format.h>
#include <spdlog/fmt/bundled/ranges.h>

// our headers
#include "common/JsonUtils.h"
#include "orchestrator/CommandParser.h"

using json = nlohmann::json;
using namespace std::string_view_literals;
using namespace PMS::JsonUtils;

namespace PMS::Orchestrator::CommandParser {

namespace {

enum class UserCommandType {
  SubmitJob,
  FindJobs,
  ResetJobs,
  FindPilots,
  CreateTask,
  CleanTask,
  ClearTask,
  DeclareTaskDependency,
  CheckTaskToken,
  Summary,
  ResetFailedJobs,
};

enum class PilotCommandType {
  ClaimJob,
  UpdateJobStatus,
  RegisterNewPilot,
  UpdateHeartBeat,
  DeleteHeartBeat,
  Test,
};

const std::unordered_map<std::string_view, UserCommandType> commandLUT{
    {"submitJob"sv, UserCommandType::SubmitJob},
    {"findJobs"sv, UserCommandType::FindJobs},
    {"resetJobs"sv, UserCommandType::ResetJobs},
    {"findPilots"sv, UserCommandType::FindPilots},
    {"createTask"sv, UserCommandType::CreateTask},
    {"clearTask"sv, UserCommandType::ClearTask},
    {"cleanTask"sv, UserCommandType::CleanTask},
    {"declareTaskDependency"sv, UserCommandType::DeclareTaskDependency},
    {"validateTaskToken"sv, UserCommandType::CheckTaskToken},
    {"summary"sv, UserCommandType::Summary},
    {"resetFailedJobs"sv, UserCommandType::ResetFailedJobs},
};

const std::unordered_map<std::string_view, PilotCommandType> pilotCommandLUT{
    {"p_claimJob"sv, PilotCommandType::ClaimJob},
    {"p_updateJobStatus"sv, PilotCommandType::UpdateJobStatus},
    {"p_registerNewPilot"sv, PilotCommandType::RegisterNewPilot},
    {"p_updateHeartBeat"sv, PilotCommandType::UpdateHeartBeat},
    {"p_deleteHeartBeat"sv, PilotCommandType::DeleteHeartBeat},
    {"p_test"sv, PilotCommandType::Test},
};

} // namespace

UserCommand toUserCommand(const json &msg) {
  if (msg.contains("livenessProbe"))
    return OrchCommand<LivenessProbe>{};

  if (!msg.contains("command") || !msg["command"].is_string())
    return OrchCommand<InvalidCommand>{"Invalid message, missing \"command\" field"};

  auto command = msg["command"].get<std::string_view>();
  auto cmdTypeP = commandLUT.find(command);

  if (cmdTypeP == end(commandLUT)) {
    if (command == "fava")
      return OrchCommand<InvalidCommand>{"Duranti, faccia il serio..."};
    return OrchCommand<InvalidCommand>{fmt::format("Command {} not supported", command)};
  }

  std::string errorMessage{};
  switch (cmdTypeP->second) {
  case UserCommandType::CreateTask:
    if (ValidateJsonCommand<CreateTask>(msg))
      return OrchCommand<CreateTask>{to_string(msg["task"])};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", CreateTask::requiredFields);
    break;
  case UserCommandType::ClearTask:
    if (ValidateJsonCommand<ClearTask>(msg))
      return OrchCommand<ClearTask>{to_string(msg["task"]), to_string(msg["token"])};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", ClearTask::requiredFields);
    break;
  case UserCommandType::CleanTask:
    if (ValidateJsonCommand<CleanTask>(msg))
      return OrchCommand<CleanTask>{to_string(msg["task"]), to_string(msg["token"])};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", CleanTask::requiredFields);
    break;
  case UserCommandType::DeclareTaskDependency:
    if (ValidateJsonCommand<DeclareTaskDependency>(msg))
      return OrchCommand<DeclareTaskDependency>{to_string(msg["task"]), to_string(msg["dependsOn"]),
                                                to_string(msg["token"])};
    errorMessage =
        fmt::format("Invalid command arguments. Required fields are: {}", DeclareTaskDependency::requiredFields);
    break;
  case UserCommandType::CheckTaskToken:
    if (ValidateJsonCommand<CheckTaskToken>(msg))
      return OrchCommand<CheckTaskToken>{to_string(msg["task"]), to_string(msg["token"])};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", CheckTaskToken::requiredFields);
    break;
  case UserCommandType::SubmitJob:
    if (ValidateJsonCommand<SubmitJob>(msg))
      return OrchCommand<SubmitJob>{msg["job"], to_string(msg["task"]), to_string(msg["token"])};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", SubmitJob::requiredFields);
    break;
  case UserCommandType::FindJobs:
    if (ValidateJsonCommand<FindJobs>(msg))
      return OrchCommand<FindJobs>{msg["match"], msg.contains("filter") ? msg["filter"] : json{}};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", FindJobs::requiredFields);
    break;
  case UserCommandType::ResetJobs:
    if (ValidateJsonCommand<ResetJobs>(msg))
      return OrchCommand<ResetJobs>{msg["match"]};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", ResetJobs::requiredFields);
    break;
  case UserCommandType::FindPilots:
    if (ValidateJsonCommand<FindPilots>(msg))
      return OrchCommand<FindPilots>{msg["match"], msg.contains("filter") ? msg["filter"] : json{}};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", FindPilots::requiredFields);
    break;
  case UserCommandType::Summary:
    if (ValidateJsonCommand<Summary>(msg))
      return OrchCommand<Summary>{to_string(msg["user"])};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", Summary::requiredFields);
    break;
  case UserCommandType::ResetFailedJobs:
    if (ValidateJsonCommand<ResetFailedJobs>(msg))
      return OrchCommand<ResetFailedJobs>{to_string(msg["task"]), to_string(msg["token"])};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", ResetFailedJobs::requiredFields);
    break;
  }

  return OrchCommand<InvalidCommand>{errorMessage};
}

PilotCommand toPilotCommand(const json &msg) {
  if (!msg.is_object() || !msg.contains("command") || !msg["command"].is_string() ||
      msg["command"].get_ref<const std::string &>().empty())
    return OrchCommand<InvalidCommand>{"Invalid message, missing \"command\" field"};

  auto command = msg["command"].get<std::string_view>();
  auto cmdTypeP = pilotCommandLUT.find(command);

  if (cmdTypeP == end(pilotCommandLUT))
    return OrchCommand<InvalidCommand>{fmt::format("Command {} not supported", command)};

  std::string errorMessage{};

  switch (cmdTypeP->second) {
  case PilotCommandType::ClaimJob:
    if (ValidateJsonCommand<ClaimJob>(msg))
      return OrchCommand<ClaimJob>{to_string(msg["pilotUuid"])};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", ClaimJob::requiredFields);
    break;
  case PilotCommandType::UpdateJobStatus:
    if (ValidateJsonCommand<UpdateJobStatus>(msg) &&
        magic_enum::enum_cast<JobStatus>(to_string_view(msg["status"])).has_value())
      return OrchCommand<UpdateJobStatus>{magic_enum::enum_cast<JobStatus>(to_string_view(msg["status"])).value(),
                                          to_string(msg["pilotUuid"]), to_string(msg["hash"]), to_string(msg["task"])};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", UpdateJobStatus::requiredFields);
    break;
  case PilotCommandType::RegisterNewPilot:
    if (ValidateJsonCommand<RegisterNewPilot>(msg)) {
      std::vector<std::pair<std::string, std::string>> tasks;
      for (const auto &task : msg["tasks"]) {
        tasks.emplace_back(task["name"], task["token"]);
      }
      std::vector<std::string> tags;
      if (msg.contains("tags")) {
        std::ranges::transform(msg["tags"], std::back_inserter(tags), [](const auto &tag) { return to_string(tag); });
      }
      return OrchCommand<RegisterNewPilot>{to_string(msg["pilotUuid"]), to_string(msg["user"]), std::move(tasks),
                                           std::move(tags), msg["host"]};
    }
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", RegisterNewPilot::requiredFields);
    break;
  case PilotCommandType::UpdateHeartBeat:
    if (ValidateJsonCommand<UpdateHeartBeat>(msg))
      return OrchCommand<UpdateHeartBeat>{to_string(msg["uuid"])};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", UpdateHeartBeat::requiredFields);
    break;
  case PilotCommandType::DeleteHeartBeat:
    if (ValidateJsonCommand<DeleteHeartBeat>(msg))
      return OrchCommand<DeleteHeartBeat>{to_string(msg["uuid"])};
    errorMessage = fmt::format("Invalid command arguments. Required fields are: {}", DeleteHeartBeat::requiredFields);
    break;
  case PilotCommandType::Test:
    return OrchCommand<Test>{};
  }

  return OrchCommand<InvalidCommand>{errorMessage};
}

} // namespace PMS::Orchestrator::CommandParser
