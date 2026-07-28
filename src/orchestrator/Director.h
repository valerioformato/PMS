#ifndef PMS_ORCHESTRATOR_DIRECTOR_H
#define PMS_ORCHESTRATOR_DIRECTOR_H

// c++ headers
#include <future>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

// external dependencies
#include <exec/static_thread_pool.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <stdexec/execution.hpp>

// our headers
#include "common/queue.h"
#include "db/backends/MongoDB/MongoDBBackend.h"
#include "db/harness/Harness.h"
#include "orchestrator/IDirector.h"
#include "orchestrator/Task.h"

using json = nlohmann::json;

namespace PMS::Orchestrator {
class Director : public IDirector {
public:
  Director(unsigned int n_io_threads)
      : m_logger{spdlog::get("Director") ? spdlog::get("Director") : spdlog::stdout_color_st("Director")},
        m_io_thread_pool{n_io_threads} {}

  // Injection constructor for testing: accepts pre-built Harness instances.
  Director(unsigned int n_io_threads, std::unique_ptr<DB::Harness> frontDB, std::unique_ptr<DB::Harness> backDB)
      : m_logger{spdlog::get("Director") ? spdlog::get("Director") : spdlog::stdout_color_st("Director")},
        m_io_thread_pool{n_io_threads}, m_frontDB{std::move(frontDB)}, m_backDB{std::move(backDB)} {}

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

  OperationResult AddNewJob(const json &job) override;
  OperationResult AddNewJob(json &&job) override;

  Async<ErrorOr<json>> PilotClaimJob(std::string_view pilotUuid) override;
  Async<ErrorOr<void>> UpdateJobStatus(std::string_view pilotUuid, std::string_view hash, std::string_view task,
                                       JobStatus status) override;

  Async<ErrorOr<NewPilotResult>> RegisterNewPilot(std::string_view pilotUuid, std::string_view user,
                                                  const std::vector<std::pair<std::string, std::string>> &tasks,
                                                  const std::vector<std::string> &tags, const json &host_info) override;
  ErrorOr<void> UpdateHeartBeat(std::string_view pilotUuid) override;
  Async<ErrorOr<void>> DeleteHeartBeat(std::string_view pilotUuid) override;

  Async<ErrorOr<void>> AddTaskDependency(const std::string &taskName, const std::string &dependsOn) override;

  Async<ErrorOr<std::string>> CreateTask(const std::string &task) override;
  Async<ErrorOr<void>> ClearTask(const std::string &task, bool deleteTask = true) override;

  Async<ErrorOr<std::string>> Summary(const std::string &user) override;

  Async<ErrorOr<std::string>> QueryBackDB(QueryOperation operation, const json &match, const json &option) override;
  Async<ErrorOr<std::string>> QueryFrontDB(DBCollection collection, const json &match, const json &filter) override;

  OperationResult ValidateTaskToken(std::string_view task, std::string_view token) const override;
  Async<ErrorOr<void>> ResetFailedJobs(std::string_view taskname) override;

private:
  void JobInsert();
  void JobTransfer();
  void UpdateTasks();
  void UpdateDeadPilots();
  void WriteJobUpdates();
  void WriteHeartBeatUpdates();
  void DBSync();

  ErrorOr<void> UpdateTaskCounts(Orchestrator::Task &task);

  unsigned int m_maxJobTransferQuerySize = 1000u;

  struct PilotInfo {
    std::string uuid;
    std::vector<std::string> tasks;
    std::vector<std::string> tags;
  };
  Async<ErrorOr<PilotInfo>> GetPilotInfo(std::string_view uuid);
  std::unordered_map<std::string, PilotInfo> m_activePilots;

  std::shared_ptr<spdlog::logger> m_logger;

  exec::static_thread_pool m_io_thread_pool;

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

public:
  // Test helper: set task totJobs so IsActive() returns true.
  void SetTaskTotJobs(std::string_view taskName, unsigned int totJobs) {
    std::string name{taskName};
    if (auto it = m_tasks.find(name); it != end(m_tasks)) {
      it->second.totJobs = totJobs;
    }
  }

  std::promise<void> m_exitSignal;
  std::shared_future<void> m_exitSignalFuture{m_exitSignal.get_future()};
  std::vector<std::thread> m_threads;

  static constexpr unsigned int m_maxRetries = 3;
};

} // namespace PMS::Orchestrator

#endif
