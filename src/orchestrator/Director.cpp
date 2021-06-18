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
  m_threads.emplace_back(&Director::JobTransfer, this);
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

Director::OperationResult Director::AddTaskDependency(const std::string &task, const std::string &dependsOn) {
  auto &thisTask = m_tasks[task];
  if (thisTask.name.empty()) {
    // this means task is newly created
    thisTask.name = task;
  }

  auto &dTask = m_tasks[dependsOn];
  if (dTask.name.empty()) {
    // this means task is newly created
    dTask.name = dependsOn;
  }

  thisTask.dependencies.push_back(dependsOn);

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
  static constexpr auto coolDown = std::chrono::milliseconds(1);

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
  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

void Director::JobTransfer() {
  static constexpr auto coolDown = std::chrono::seconds(10);

  auto bHandle = m_backPoolHandle->DBHandle();
  auto fHandle = m_frontPoolHandle->DBHandle();

  std::vector<bsoncxx::document::view_or_value> toBeInserted;
  do {
    // collect jobs from each active task
    for (const auto &task : m_tasks) {
      if (!task.second.readyForScheduling || !task.second.IsActive()) {
        continue;
      }

      json getJobsQuery;
      getJobsQuery["task"] = task.second.name;
      getJobsQuery["inFrontDB"]["$exists"] = false;

      auto queryResult = bHandle["jobs"].find(JsonUtils::json2bson(getJobsQuery));
      std::copy(queryResult.begin(), queryResult.end(), begin(toBeInserted));
    }

    if (!toBeInserted.empty()) {
      m_logger->debug("Inserting {} new jobs into backend DB", toBeInserted.size());
      fHandle["jobs"].insert_many(toBeInserted);

      for (const auto &jobIt : toBeInserted) {
        json job = JsonUtils::bson2json(jobIt);

        json updateFilter;
        updateFilter["hash"] = job["hash"];

        json updateAction;
        updateAction["$set"]["inFrontDB"] = true;

        bHandle["jobs"].update_one(JsonUtils::json2bson(updateFilter), JsonUtils::json2bson(updateAction));
      }

      toBeInserted.clear();
    }
  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

void Director::UpdateTasks() {
  static constexpr auto coolDown = std::chrono::seconds(60);

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

      if (task.dependencies.empty()) {
        task.readyForScheduling = true;
      } else {
        bool taskIsReady = true;
        for (const auto &requiredTaskName : task.dependencies) {
          auto requiredTaskIt = m_tasks.find(requiredTaskName);
          if (requiredTaskIt == end(m_tasks)) {
            m_logger->warn("Task {} required by task {} not found in internal map", requiredTaskName, taskName);
            task.readyForScheduling = false;
            break;
          }

          taskIsReady &= requiredTaskIt->second.IsFinished();
        }

        task.readyForScheduling = taskIsReady;
      }

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

  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}
} // namespace Orchestrator
} // namespace PMS