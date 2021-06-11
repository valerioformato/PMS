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

// our headers
#include "common/queue.h"
#include "db/PoolHandle.h"
#include "orchestrator/Task.h"

using json = nlohmann::json;

namespace PMS {
namespace Orchestrator {
class Director {
public:
  Director(std::shared_ptr<DB::PoolHandle> frontPoolHandle, std::shared_ptr<DB::PoolHandle> backPoolHandle)
      : m_frontPoolHandle{std::move(frontPoolHandle)}, m_backPoolHandle{std::move(backPoolHandle)} {}

  void Start();
  void Stop();

  void AddNewJob(const json &job) { m_incomingJobs.push(job); };
  void AddNewJob(json &&job) { m_incomingJobs.push(job); };

private:
  void JobInsert();
  void UpdateTasks();

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