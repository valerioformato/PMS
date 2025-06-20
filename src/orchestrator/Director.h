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
#include "db/backends/MongoDB/MongoDBBackend.h"
#include "db/backends/MongoDB/PoolHandle.h"
#include "db/harness/Harness.h"
#include "orchestrator/Task.h"

using json = nlohmann::json;

namespace PMS::Orchestrator {
class Director {
public:
  Director() : m_logger{spdlog::stdout_color_st("Director")} {}

  void Start();
  void Stop();

  void SetFrontDB(std::string_view dbhost, std::string_view dbname) {
    spdlog::info("Using frontend DB: {}/{}", dbhost, dbname);
    m_frontDB = std::make_unique<DB::Harness>(std::make_unique<DB::MongoDBBackend>(dbhost, dbname));
  }
  void SetBackDB(std::string_view dbhost, std::string_view dbname) {
    spdlog::info("Using backend DB: {}/{}", dbhost, dbname);
    m_backDB = std::make_unique<DB::Harness>(std::make_unique<DB::MongoDBBackend>(dbhost, dbname));
  }
  void SetMaxJobTransferQuerySize(unsigned int size) { m_maxJobTransferQuerySize = size; }

  enum class OperationResult { Success, ProcessError, DatabaseError };

  OperationResult AddNewJob(const json &job);
  OperationResult AddNewJob(json &&job);

  ErrorOr<json> PilotClaimJob(std::string_view pilotUuid);
  ErrorOr<void> UpdateJobStatus(std::string_view pilotUuid, std::string_view hash, std::string_view task,
                                JobStatus status);

  struct NewPilotResult {
    OperationResult result;
    std::vector<std::string> validTasks;
    std::vector<std::string> invalidTasks;
  };
  ErrorOr<NewPilotResult> RegisterNewPilot(std::string_view pilotUuid, std::string_view user,
                                           const std::vector<std::pair<std::string, std::string>> &tasks,
                                           const std::vector<std::string> &tags, const json &host_info);
  ErrorOr<void> UpdateHeartBeat(std::string_view pilotUuid);
  ErrorOr<void> DeleteHeartBeat(std::string_view pilotUuid);

  ErrorOr<void> AddTaskDependency(const std::string &taskName, const std::string &dependsOn);

  ErrorOr<std::string> CreateTask(const std::string &task);
  ErrorOr<void> ClearTask(const std::string &task, bool deleteTask = true);

  ErrorOr<std::string> Summary(const std::string &user) const;

  enum class DBCollection { Jobs, Pilots };
  enum class QueryOperation { Find, UpdateOne, UpdateMany, DeleteOne, DeleteMany };
  ErrorOr<std::string> QueryBackDB(QueryOperation operation, const json &match, const json &option) const;
  ErrorOr<std::string> QueryFrontDB(DBCollection collection, const json &match, const json &filter) const;

  OperationResult ValidateTaskToken(std::string_view task, std::string_view token) const;
  ErrorOr<void> ResetFailedJobs(std::string_view taskname);

private:
  void JobInsert();
  void JobTransfer();
  void UpdateTasks();
  void UpdateDeadPilots();
  void WriteJobUpdates();
  void WriteHeartBeatUpdates();
  void DBSync();

  ErrorOr<void> UpdateTaskCounts(Task &task);

  unsigned int m_maxJobTransferQuerySize = 1000u;

  struct PilotInfo {
    std::string uuid;
    std::vector<std::string> tasks;
    std::vector<std::string> tags;
  };
  ErrorOr<PilotInfo> GetPilotInfo(std::string_view uuid);
  std::unordered_map<std::string, PilotInfo> m_activePilots;

  void RunClaimQueries();
  ts_queue<PilotInfo> m_claimRequests;
  struct ClaimedJob {
    bool sent = false;
    bool claimed = false;
    json job;
  };
  std::unordered_map<std::string, ClaimedJob> m_claimedJobs;

  std::shared_ptr<spdlog::logger> m_logger;

  std::unique_ptr<DB::Harness> m_frontDB;
  std::unique_ptr<DB::Harness> m_backDB;

  ts_queue<json> m_incomingJobs;

  using time_point = std::chrono::system_clock::time_point;
  struct PilotHeartBeat {
    PilotHeartBeat(std::string u, time_point t) : uuid{std::move(u)}, time{t} {};
    std::string uuid;
    time_point time;
  };
  ts_queue<PilotHeartBeat> m_heartbeatUpdates;

  std::mutex m_jobUpdateRequests_mx;
  std::vector<DB::Queries::Query> m_jobUpdateRequests;

  std::unordered_map<std::string, Task> m_tasks;

  std::promise<void> m_exitSignal;
  std::shared_future<void> m_exitSignalFuture{m_exitSignal.get_future()};
  std::vector<std::thread> m_threads;

  static constexpr unsigned int m_maxRetries = 3;
};

} // namespace PMS::Orchestrator

#endif
