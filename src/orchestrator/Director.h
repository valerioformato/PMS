#ifndef PMS_ORCHESTRATOR_DIRECTOR_H
#define PMS_ORCHESTRATOR_DIRECTOR_H

// c++ headers
#include <future>
#include <queue>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

// external dependencies
#include <nlohmann/json.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

// our headers
#include "common/queue.h"
#include "db/PoolHandle.h"
#include "orchestrator/Task.h"

using json = nlohmann::json;

namespace PMS::Orchestrator {
class Director {
public:
  enum class OperationResult { Success, ProcessError, DatabaseError };

  Director(std::shared_ptr<DB::PoolHandle> frontPoolHandle, std::shared_ptr<DB::PoolHandle> backPoolHandle)
      : m_logger{spdlog::stdout_color_st("Director")}, m_frontPoolHandle{std::move(frontPoolHandle)},
        m_backPoolHandle{std::move(backPoolHandle)} {}

  void Start();
  void Stop();

  OperationResult AddNewJob(const json &job);
  OperationResult AddNewJob(json &&job);

  json ClaimJob(std::string_view pilotUuid);
  OperationResult UpdateJobStatus(std::string_view pilotUuid, std::string_view hash, std::string_view task,
                                  JobStatus status);

  struct NewPilotResult {
    OperationResult result;
    std::vector<std::string> validTasks;
    std::vector<std::string> invalidTasks;
  };
  NewPilotResult RegisterNewPilot(std::string_view pilotUuid, std::string_view user,
                                  const std::vector<std::pair<std::string, std::string>> &tasks,
                                  const std::vector<std::string> &tags);
  OperationResult UpdateHeartBeat(std::string_view pilotUuid);
  OperationResult DeleteHeartBeat(std::string_view pilotUuid);

  OperationResult AddTaskDependency(const std::string &taskName, const std::string &dependsOn);

  struct CreateTaskResult {
    OperationResult result;
    std::string token;
  };
  CreateTaskResult CreateTask(const std::string &task);
  OperationResult ClearTask(const std::string &task, bool deleteTask = true);

  std::string Summary(const std::string &user);

  bool ValidateTaskToken(std::string_view task, std::string_view token) const;

private:
  void JobInsert();
  void JobTransfer();
  void UpdateTasks();
  void UpdatePilots();
  void DBSync();

  struct PilotInfo {
    std::vector<std::string> tasks;
    std::vector<std::string> tags;
  };
  PilotInfo GetPilotInfo(std::string_view uuid);
  std::unordered_map<std::string, PilotInfo> m_activePilots;

  std::shared_ptr<spdlog::logger> m_logger;

  std::shared_ptr<DB::PoolHandle> m_frontPoolHandle;
  std::shared_ptr<DB::PoolHandle> m_backPoolHandle;

  ts_queue<json> m_incomingJobs;

  std::unordered_map<std::string, Task> m_tasks;

  std::promise<void> m_exitSignal;
  std::shared_future<void> m_exitSignalFuture{m_exitSignal.get_future()};
  std::vector<std::thread> m_threads;
};

} // namespace PMS::Orchestrator

#endif