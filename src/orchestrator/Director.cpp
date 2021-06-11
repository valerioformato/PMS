// c++ headers
#include <algorithm>

// external headers
#include <fmt/ostream.h>
#include <mongocxx/pipeline.hpp>
#include <nlohmann/json.hpp>

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

Director::OperationResult Director::AddNewJob(const json &job) {
  m_incomingJobs.push(job);
  return OperationResult::Success;
}

Director::OperationResult Director::AddNewJob(json &&job) {
  m_incomingJobs.push(job);
  return OperationResult::Success;
}

Director::OperationResult Director::CleanTask(const std::string &task) const {
  auto bHandle = m_backPoolHandle->DBHandle();
  auto fHandle = m_frontPoolHandle->DBHandle();

  json deleteQuery;
  deleteQuery["task"] = task;
  try {
    m_logger->debug("Cleaning task {}", task);
    bHandle["jobs"].delete_many(JsonUtils::json2bson(deleteQuery));
    fHandle["jobs"].delete_many(JsonUtils::json2bson(deleteQuery));
  } catch (const std::exception &e) {
    m_logger->error("Server query failed with error {}", e.what());
    return OperationResult::DatabaseError;
  }

  return OperationResult::Success;
}

void Director::JobInsert() {
  auto handle = m_backPoolHandle->DBHandle();

  std::vector<bsoncxx::document::value> toBeInserted;
  do {
    while (!m_incomingJobs.empty()) {
      auto job = m_incomingJobs.consume();

      json jobQuery;
      jobQuery["task"] = job["task"];
      jobQuery["hash"] = job["hash"];

      // check if this job is already in back-end database
      auto queryResult = handle["jobs"].find_one(JsonUtils::json2bson(jobQuery));
      if (queryResult)
        continue;

      m_logger->trace("Queueing up job {} for insertion", job["hash"]);

      // job initial status should always be Pending :)
      job["status"] = JobStatusNames[JobStatus::Pending];
      toBeInserted.push_back(JsonUtils::json2bson(job));
    }

    if (!toBeInserted.empty()) {
      m_logger->debug("Inserting {} new jobs into backend DB", toBeInserted.size());
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
    m_logger->debug("Updating tasks");

    // the pipeline must be re-created from scratch
    mongocxx::pipeline aggregationPipeline;

    // get list of all tasks
    auto tasksResult = handle["jobs"].aggregate(aggregationPipeline.group(JsonUtils::json2bson(taskAggregateQuery)));

    std::vector<std::string> dbTasks;

    // beware: cursors cannot be reused as-is
    for (const auto &result : tasksResult) {
      json tmpdoc = JsonUtils::bson2json(result);

      // find task in internal task list
      std::string taskName = tmpdoc["_id"];

      dbTasks.emplace_back(taskName);

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

      m_logger->debug("Task {0} updated - {1} job{4} ({2} done, {3} failed)", task.name, task.totJobs, task.doneJobs,
                      task.failedJobs, task.totJobs > 1 ? "s" : "");
    }

    // check if there are tasks in the internal representation that are no longer in the DB
    std::vector<std::string> deletedTasks;
    for (const auto &taskIt : m_tasks) {
      if (std::find(begin(dbTasks), end(dbTasks), taskIt.first) == end(dbTasks))
        deletedTasks.push_back(taskIt.first);
    }

    // ... and then remove them!
    std::for_each(begin(deletedTasks), end(deletedTasks), [this](const auto &taskName) {
      m_logger->debug("Removing task {}", taskName);
      m_tasks.erase(m_tasks.find(taskName));
    });

  } while (m_exitSignalFuture.wait_for(std::chrono::seconds(60)) == std::future_status::timeout);
}
} // namespace Orchestrator
} // namespace PMS