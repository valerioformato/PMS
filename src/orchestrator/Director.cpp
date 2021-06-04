// c++ headers
#include <algorithm>

// external headers
#include <fmt/ostream.h>
#include <mongocxx/pipeline.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "orchestrator/Director.h"
#include "common/JsonUtils.h"
#include "common/Job.h"

using json = nlohmann::json;

namespace PMS {
namespace Orchestrator {
void Director::Start() { m_backPoolHandle->DBHandle().SetupJobIndexes(); }
void Director::Stop() {}
void Director::UpdateTasks() {
  auto handle = m_backPoolHandle->DBHandle();

  json taskAggregateQuery;
  taskAggregateQuery["_id"] = "$task";

  mongocxx::pipeline aggregationPipeline;

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
} // namespace Orchestrator
} // namespace PMS