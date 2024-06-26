//
// Created by Valerio Formato on 20/07/21.
//

#ifndef PMS_USERCOMMANDS_H
#define PMS_USERCOMMANDS_H

// c++ headers
#include <string>
#include <variant>

// external dependencies
#include <nlohmann/json.hpp>

using json = nlohmann::json;
namespace PMS::Orchestrator {

using namespace std::string_view_literals;

struct SubmitJob {
  json job;
  std::string task;
  std::string token;

  constexpr static std::array requiredFields{"job"sv, "task"sv, "token"sv};
};

struct FindJobs {
  json match;
  json filter;

  constexpr static std::array requiredFields{"match"sv};
};

struct ResetJobs {
  json match;

  constexpr static std::array requiredFields{"match"sv};
};

struct FindPilots {
  json match;
  json filter;

  constexpr static std::array requiredFields{"match"sv};
};

struct CreateTask {
  std::string task;

  constexpr static std::array requiredFields{"task"sv};
};

struct ClearTask {
  std::string task;
  std::string token;

  constexpr static std::array requiredFields{"task"sv, "token"sv};
};

struct CleanTask {
  std::string task;
  std::string token;

  constexpr static std::array requiredFields{"task"sv, "token"sv};
};

struct DeclareTaskDependency {
  std::string task;
  std::string dependsOn;
  std::string token;

  constexpr static std::array requiredFields{"task"sv, "dependsOn"sv, "token"sv};
};

struct CheckTaskToken {
  std::string task;
  std::string token;

  constexpr static std::array requiredFields{"task"sv, "token"sv};
};

struct Summary {
  std::string user;

  constexpr static std::array requiredFields{"user"sv};
};

struct ResetFailedJobs {
  std::string task;
  std::string token;

  constexpr static std::array requiredFields{"task"sv, "token"sv};
};

} // namespace PMS::Orchestrator
#endif // PMS_USERCOMMANDS_H
