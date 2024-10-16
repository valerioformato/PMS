#ifndef PMS_ORCHESTRATOR_DIRECTOR_H
#define PMS_ORCHESTRATOR_DIRECTOR_H

// c++ headers
#include <future>
#include <mutex>
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
#include "db/backends/MongoDB/PoolHandle.h"
#include "orchestrator/Task.h"

using json = nlohmann::json;

namespace PMS::Orchestrator {
class Director {
public:
  using time_point = std::chrono::system_clock::time_point;

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
                                  const std::vector<std::string> &tags, const json &host_info);
  OperationResult UpdateHeartBeat(std::string_view pilotUuid);
  OperationResult DeleteHeartBeat(std::string_view pilotUuid);

  OperationResult AddTaskDependency(const std::string &taskName, const std::string &dependsOn);

  struct CreateTaskResult {
    OperationResult result;
    std::string token;
  };
  CreateTaskResult CreateTask(const std::string &task);
  OperationResult ClearTask(const std::string &task, bool deleteTask = true);

  std::string Summary(const std::string &user) const;

  struct QueryResult {
    OperationResult result;
    std::string msg;
  };
  enum class DBCollection { Jobs, Pilots };
  enum class QueryOperation { Find, UpdateOne, UpdateMany, DeleteOne, DeleteMany };
  QueryResult QueryBackDB(QueryOperation operation, const json &match, const json &option) const;
  QueryResult QueryFrontDB(DBCollection collection, const json &match, const json &filter) const;

  OperationResult ValidateTaskToken(std::string_view task, std::string_view token) const;
  OperationResult ResetFailedJobs(std::string_view taskname);

private:
  void JobInsert();
  void JobTransfer();
  void UpdateTasks();
  void UpdateDeadPilots();
  void UpdateTaskCounts(Task &task);
  void WriteJobUpdates();
  void WriteHeartBeatUpdates();
  void DBSync();

  struct PilotInfo {
    std::vector<std::string> tasks;
    std::vector<std::string> tags;
  };
  std::optional<PilotInfo> GetPilotInfo(std::string_view uuid);
  std::unordered_map<std::string, PilotInfo> m_activePilots;

  std::shared_ptr<spdlog::logger> m_logger;

  std::shared_ptr<DB::PoolHandle> m_frontPoolHandle;
  std::shared_ptr<DB::PoolHandle> m_backPoolHandle;

  ts_queue<json> m_incomingJobs;

  struct PilotHeartBeat {
    PilotHeartBeat(std::string u, time_point t) : uuid{std::move(u)}, time{t} {};
    std::string uuid;
    time_point time;
  };
  ts_queue<PilotHeartBeat> m_heartbeatUpdates;

  std::mutex m_jobUpdateRequests_mx;
  std::vector<mongocxx::model::write> m_jobUpdateRequests;

  std::unordered_map<std::string, Task> m_tasks;

  std::promise<void> m_exitSignal;
  std::shared_future<void> m_exitSignalFuture{m_exitSignal.get_future()};
  std::vector<std::thread> m_threads;

  static constexpr unsigned int m_maxRetries = 3;
};

} // namespace PMS::Orchestrator

#endif
