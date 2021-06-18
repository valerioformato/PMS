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

namespace PMS {
namespace Orchestrator {
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

  OperationResult AddTaskDependency(const std::string &taskName, const std::string &dependsOn);

  OperationResult CleanTask(const std::string &task) const;

private:
  void JobInsert();
  void UpdateTasks();

  std::shared_ptr<spdlog::logger> m_logger;

  std::shared_ptr<DB::PoolHandle> m_frontPoolHandle;
  std::shared_ptr<DB::PoolHandle> m_backPoolHandle;

  ts_queue<json> m_incomingJobs;

  std::unordered_map<std::string, Task> m_tasks;

  std::promise<void> m_exitSignal;
  std::shared_future<void> m_exitSignalFuture{m_exitSignal.get_future()};
  std::vector<std::thread> m_threads;
};

} // namespace Orchestrator
} // namespace PMS

#endif