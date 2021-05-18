#ifndef PMS_ORCHESTRATOR_DIRECTOR_H
#define PMS_ORCHESTRATOR_DIRECTOR_H

// c++ headers
#include <queue>
#include <vector>

// external dependencies
#include <nlohmann/json.hpp>

// our headers
#include "db/PoolHandle.h"
#include "orchestrator/Task.h"

using json = nlohmann::json;

namespace PMS {
namespace Orchestrator {
class Director {
public:
  Director(std::shared_ptr<DB::PoolHandle> frontPoolHandle, std::shared_ptr<DB::PoolHandle> backPoolHandle)
      : m_frontPoolHandle{frontPoolHandle}, m_backPoolHandle{backPoolHandle} {}

  void Start();
  void Stop();

  void AddNewJob(const json &job) { m_incomingJobs.push(job); };
  void AddNewJob(json &&job) { m_incomingJobs.push(job); };

private:
  void UpdateTasks();

  std::shared_ptr<DB::PoolHandle> m_frontPoolHandle;
  std::shared_ptr<DB::PoolHandle> m_backPoolHandle;

  std::queue<json> m_incomingJobs;

  std::vector<Task> m_tasks;
};

} // namespace Orchestrator
} // namespace PMS

#endif