//
// Created by Valerio Formato on 20/07/21.
//

#ifndef PMS_PILOTCOMMANDS_H
#define PMS_PILOTCOMMANDS_H

// c++ headers
#include <string>
#include <variant>

// external dependencies
#include <nlohmann/json.hpp>

namespace PMS::Orchestrator {

using namespace std::string_view_literals;

struct ClaimJob {
  std::string uuid;

  constexpr static std::array requiredFields{"pilotUuid"sv};
};

struct UpdateJobStatus {
  JobStatus status;
  std::string uuid;
  std::string hash;
  std::string task;

  constexpr static std::array requiredFields{"pilotUuid"sv, "status"sv, "hash"sv, "task"sv};
};

struct RegisterNewPilot {
  std::string uuid;
  std::string user;
  std::vector<std::pair<std::string, std::string>> tasks;

  constexpr static std::array requiredFields{"pilotUuid"sv, "user"sv, "tasks"sv};
};

struct UpdateHeartBeat {
  std::string uuid;

  constexpr static std::array requiredFields{"uuid"sv};
};

struct DeleteHeartBeat {
  std::string uuid;

  constexpr static std::array requiredFields{"uuids"sv};
};
} // namespace PMS::Orchestrator
#endif // PMS_PILOTCOMMANDS_H
