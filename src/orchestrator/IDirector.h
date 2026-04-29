#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>
#include <stdexec/execution.hpp>

#include "common/Job.h"
#include "common/Types/Error.h"

using json = nlohmann::json;

namespace PMS::Orchestrator {

class IDirector {
public:
  virtual ~IDirector() = default;

  template <typename T> using Async = stdexec::task<T>;

  enum class OperationResult { Success, ProcessError, DatabaseError };

  virtual OperationResult AddNewJob(const json &job) = 0;
  virtual OperationResult AddNewJob(json &&job) = 0;

  virtual Async<ErrorOr<json>> PilotClaimJob(std::string_view pilotUuid) = 0;
  virtual Async<ErrorOr<void>> UpdateJobStatus(std::string_view pilotUuid, std::string_view hash, std::string_view task,
                                               JobStatus status) = 0;

  struct NewPilotResult {
    OperationResult result;
    std::vector<std::string> validTasks;
    std::vector<std::string> invalidTasks;
  };
  virtual Async<ErrorOr<NewPilotResult>> RegisterNewPilot(std::string_view pilotUuid, std::string_view user,
                                                          const std::vector<std::pair<std::string, std::string>> &tasks,
                                                          const std::vector<std::string> &tags,
                                                          const json &host_info) = 0;

  virtual ErrorOr<void> UpdateHeartBeat(std::string_view pilotUuid) = 0;
  virtual Async<ErrorOr<void>> DeleteHeartBeat(std::string_view pilotUuid) = 0;

  virtual Async<ErrorOr<void>> AddTaskDependency(const std::string &taskName, const std::string &dependsOn) = 0;

  virtual Async<ErrorOr<std::string>> CreateTask(const std::string &task) = 0;
  virtual Async<ErrorOr<void>> ClearTask(const std::string &task, bool deleteTask = true) = 0;

  virtual Async<ErrorOr<std::string>> Summary(const std::string &user) const = 0;

  enum class DBCollection { Jobs, Pilots };
  enum class QueryOperation { Find, UpdateOne, UpdateMany, DeleteOne, DeleteMany };

  virtual Async<ErrorOr<std::string>> QueryBackDB(QueryOperation operation, const json &match,
                                                  const json &option) const = 0;
  virtual Async<ErrorOr<std::string>> QueryFrontDB(DBCollection collection, const json &match,
                                                   const json &filter) const = 0;

  virtual OperationResult ValidateTaskToken(std::string_view task, std::string_view token) const = 0;
  virtual Async<ErrorOr<void>> ResetFailedJobs(std::string_view taskname) = 0;
};

} // namespace PMS::Orchestrator
