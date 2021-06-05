// c++ headers
#include <algorithm>

// external headers
#include <fmt/ostream.h>
#include <mongocxx/pipeline.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "common/Job.h"
#include "common/JsonUtils.h"
#include "orchestrator/Director.h"

using json = nlohmann::json;

namespace PMS {
namespace Orchestrator {
void Director::Start() {
  m_backPoolHandle->DBHandle().SetupJobIndexes();

  m_threads.emplace_back(&Director::UpdateTasks, this, m_exitSignal.get_future());
}

void Director::Stop() {
  m_exitSignal.set_value();

  for (auto &thread : m_threads)
    thread.join();
}

void Director::UpdateTasks(std::future<void> exitSignal) {
  auto handle = m_backPoolHandle->DBHandle();

  json taskAggregateQuery;
  taskAggregateQuery["_id"] = "$task";

  mongocxx::pipeline aggregationPipeline;

  while (exitSignal.wait_for(std::chrono::seconds(60)) == std::future_status::timeout) {
    // get list of all tasks
    auto tasksResult = handle["jobs"].aggregate(aggregationPipeline.group(bsoncxx::from_json(taskAggregateQuery)));
    for (const auto &result : tasksResult) {
      json tmpdoc = JsonUtils::bson2json(result);
      spdlog::trace("{}", tmpdoc.dump(2));

      // find task in internal task list
      std::string taskName = tmpdoc["_id"];
      Task &task = m_tasks[taskName];
      if (task.name.empty())
        task.name = taskName;

      // update job counters in task
      json countQuery;
      countQuery["task"] = taskName;
      task.totJobs = handle["jobs"].count_documents(JsonUtils::json2bson(countQuery));

      countQuery["status"] = JobStatusNames[JobStatus::Done];
      task.doneJobs = handle["jobs"].count_documents(JsonUtils::json2bson(countQuery));

      countQuery["status"] = JobStatusNames[JobStatus::Error];
      task.failedJobs = handle["jobs"].count_documents(JsonUtils::json2bson(countQuery));
    }
  }
}
} // namespace Orchestrator
} // namespace PMS