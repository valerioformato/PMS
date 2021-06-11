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

  m_threads.emplace_back(&Director::UpdateTasks, this);
  m_threads.emplace_back(&Director::JobInsert, this);
}

void Director::Stop() {
  m_exitSignal.set_value();

  for (auto &thread : m_threads)
    thread.join();
}

void Director::JobInsert() {
  auto handle = m_backPoolHandle->DBHandle();

  std::vector<bsoncxx::document::value> toBeInserted;
  do {
    // spdlog::trace("queue size = {}", m_incomingJobs.size());
    while (!m_incomingJobs.empty()) {
      auto job = m_incomingJobs.consume();

      json jobQuery;
      jobQuery["task"] = job["task"];
      jobQuery["hash"] = job["hash"];

      // check if this job is already in back-end database
      auto queryResult = handle["jobs"].find_one(JsonUtils::json2bson(jobQuery));
      if (queryResult)
        continue;

      spdlog::trace("Director: Queueing up job {} for insertion", job["hash"]);

      toBeInserted.push_back(JsonUtils::json2bson(job));
    }

    if (!toBeInserted.empty()) {
      spdlog::debug("Inserting {} new jobs into backend DB", toBeInserted.size());
      handle["jobs"].insert_many(toBeInserted);

      toBeInserted.clear();
    }
  } while (m_exitSignalFuture.wait_for(std::chrono::milliseconds(1)) == std::future_status::timeout);
}

void Director::UpdateTasks() {
  auto handle = m_backPoolHandle->DBHandle();

  json taskAggregateQuery;
  taskAggregateQuery["_id"] = "$task";

  do {
    spdlog::debug("Updating tasks");

    // the pipeline must be re-created from scratch
    mongocxx::pipeline aggregationPipeline;

    // get list of all tasks
    auto tasksResult = handle["jobs"].aggregate(aggregationPipeline.group(JsonUtils::json2bson(taskAggregateQuery)));

    // beware: cursors cannot be reused as-is
    for (const auto &result : tasksResult) {
      json tmpdoc = JsonUtils::bson2json(result);

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

      spdlog::debug("Task {0} updated - {1} job{4} ({2} done, {3} failed)", task.name, task.totJobs, task.doneJobs,
                    task.failedJobs, task.totJobs > 1 ? "s" : "");
    }
  } while (m_exitSignalFuture.wait_for(std::chrono::seconds(60)) == std::future_status::timeout);
}
} // namespace Orchestrator
} // namespace PMS