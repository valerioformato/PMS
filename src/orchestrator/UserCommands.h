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

namespace PMS::Orchestrator {

using namespace std::string_view_literals;

struct SubmitJob {
  json job;
  std::string task;
  std::string token;

  constexpr static std::array requiredFields{"job"sv, "task"sv, "token"sv};
};

struct CreateTask {
  std::string task;

  constexpr static std::array requiredFields{"task"sv};
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
} // namespace PMS::Orchestrator
#endif // PMS_USERCOMMANDS_H
