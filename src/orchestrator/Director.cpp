// c++ headers
#include <algorithm>

// external headers
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/ranges.h>
#include <mongocxx/pipeline.hpp>
#include <nlohmann/json.hpp>

// our headers
#include "common/Job.h"
#include "common/JsonUtils.h"
#include "orchestrator/Director.h"

using json = nlohmann::json;

namespace PMS::Orchestrator {
void Director::Start() {
  m_backPoolHandle->DBHandle().SetupDBCollections();

  m_threads.emplace_back(&Director::UpdatePilots, this);
  m_threads.emplace_back(&Director::UpdateTasks, this);
  m_threads.emplace_back(&Director::JobInsert, this);
  m_threads.emplace_back(&Director::JobTransfer, this);
  m_threads.emplace_back(&Director::DBSync, this);
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

json Director::ClaimJob(std::string_view pilotUuid) {
  auto handle = m_frontPoolHandle->DBHandle();

  auto tasks = GetPilotTasks(pilotUuid);
  m_logger->trace("Pilot {} allowed tasks: {}", pilotUuid, tasks);

  json filter;
  filter["status"] = magic_enum::enum_name(JobStatus::Pending);
  filter["task"]["$in"] = tasks;

  json updateAction;
  updateAction["$set"]["status"] = magic_enum::enum_name(JobStatus::Claimed);
  updateAction["$set"]["pilotUuid"] = pilotUuid;

  auto query_result =
      handle["jobs"].find_one_and_update(JsonUtils::json2bson(filter), JsonUtils::json2bson(updateAction));

  if (query_result) {
    // remove _id field before returning
    return JsonUtils::bson2json(JsonUtils::filter(
        query_result.value(), [](const bsoncxx::document::element &el) { return el.key().to_string() == "_id"; }));
  } else {
    return {};
  }
}

Director::OperationResult Director::UpdateJobStatus(std::string_view pilotUuid, std::string_view hash,
                                                    std::string_view task, JobStatus status) {
  auto handle = m_frontPoolHandle->DBHandle();

  auto pilotTasks = GetPilotTasks(pilotUuid);
  if (std::find(begin(pilotTasks), end(pilotTasks), task) == end(pilotTasks))
    return OperationResult::DatabaseError;

  return handle.UpdateJobStatus(hash, task, status) ? OperationResult::Success : OperationResult::DatabaseError;
}

Director::NewPilotResult Director::RegisterNewPilot(std::string_view pilotUuid, std::string_view user,
                                                    const std::vector<std::pair<std::string, std::string>> &tasks) {
  NewPilotResult result{OperationResult::Success, {}, {}};

  auto handle = m_frontPoolHandle->DBHandle();

  json query;
  query["uuid"] = pilotUuid;
  query["user"] = user;
  query["tasks"] = json::array({});
  for (const auto &[taskName, token] : tasks) {
    if (ValidateTaskToken(taskName, token)) {
      query["tasks"].push_back(taskName);
      result.validTasks.push_back(taskName);
    } else {
      result.invalidTasks.push_back(taskName);
    }
  }

  auto query_result = handle["pilots"].insert_one(JsonUtils::json2bson(query));
  return bool(query_result) ? result : NewPilotResult{OperationResult::DatabaseError, {}, {}};
}

Director::OperationResult Director::UpdateHeartBeat(std::string_view pilotUuid) {
  auto handle = m_frontPoolHandle->DBHandle();

  json updateFilter;
  updateFilter["uuid"] = pilotUuid;

  json updateAction;
  updateAction["$currentDate"]["lastHeartBeat"] = true;

  mongocxx::options::update updateOpt{};
  updateOpt.upsert(true);

  auto queryResult =
      handle["pilots"].update_one(JsonUtils::json2bson(updateFilter), JsonUtils::json2bson(updateAction), updateOpt);

  return queryResult ? Director::OperationResult::Success : Director::OperationResult::DatabaseError;
}

Director::OperationResult Director::DeleteHeartBeat(std::string_view pilotUuid) {
  auto handle = m_frontPoolHandle->DBHandle();

  json deleteFilter;
  deleteFilter["uuid"] = pilotUuid;

  auto queryResult = handle["pilots"].delete_one(JsonUtils::json2bson(deleteFilter));

  return queryResult ? Director::OperationResult::Success : Director::OperationResult::DatabaseError;
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

Director::CreateTaskResult Director::CreateTask(const std::string &task) {
  if (m_tasks.find(task) != end(m_tasks)) {
    return {OperationResult::DatabaseError, ""};
  }

  m_logger->trace("Creating task {}", task);

  // generate a random token
  std::string token = boost::uuids::to_string(boost::uuids::random_generator()());

  auto &newTask = m_tasks[task];
  newTask.name = task;
  newTask.token = token;

  // insert new task in backend DB
  auto handle = m_backPoolHandle->DBHandle();

  json insertQuery;
  insertQuery["name"] = newTask.name;
  insertQuery["token"] = newTask.token;

  handle["tasks"].insert_one(JsonUtils::json2bson(insertQuery));

  return {OperationResult::Success, token};
}

Director::OperationResult Director::CleanTask(const std::string &task) {
  auto bHandle = m_backPoolHandle->DBHandle();
  auto fHandle = m_frontPoolHandle->DBHandle();

  try {
    json jobDeleteQuery;
    jobDeleteQuery["task"] = task;

    m_logger->debug("Cleaning task {}", task);
    bHandle["jobs"].delete_many(JsonUtils::json2bson(jobDeleteQuery));
    fHandle["jobs"].delete_many(JsonUtils::json2bson(jobDeleteQuery));

    json taskDeleteQuery;
    taskDeleteQuery["name"] = task;

    bHandle["tasks"].delete_many(JsonUtils::json2bson(taskDeleteQuery));
    m_tasks.erase(task);
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
      job["status"] = magic_enum::enum_name(JobStatus::Pending);
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
      for (const auto &job : queryResult) {
        // filtering out the _id field
        toBeInserted.push_back(
            JsonUtils::filter(job, [](const bsoncxx::document::element &el) { return el.key().to_string() == "_id"; }));
      }
    }

    if (!toBeInserted.empty()) {
      m_logger->debug("Inserting {} new jobs into frontend DB", toBeInserted.size());
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

  do {
    m_logger->debug("Updating tasks");

    // get list of all tasks
    auto tasksResult = handle["tasks"].find({});

    // beware: cursors cannot be reused as-is
    for (const auto &result : tasksResult) {
      json tmpdoc = JsonUtils::bson2json(result);

      // find task in internal task list
      std::string taskName = tmpdoc["name"];

      Task &task = m_tasks[taskName];
      if (task.name.empty()) {
        m_logger->warn("Task {} is in DB but was not found in memory", taskName);

        task.name = tmpdoc["name"];
        task.token = tmpdoc["token"];
      }

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

      countQuery["status"] = magic_enum::enum_name(JobStatus::Done);
      task.doneJobs = handle["jobs"].count_documents(JsonUtils::json2bson(countQuery));

      countQuery["status"] = magic_enum::enum_name(JobStatus::Error);
      task.failedJobs = handle["jobs"].count_documents(JsonUtils::json2bson(countQuery));

      m_logger->debug("Task {0} updated - {1} job{4} ({2} done, {3} failed)", task.name, task.totJobs, task.doneJobs,
                      task.failedJobs, task.totJobs > 1 ? "s" : "");
    }

  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

using time_point = std::chrono::system_clock::time_point;

void Director::UpdatePilots() {
  static constexpr auto coolDown = std::chrono::seconds(60);

  static std::chrono::system_clock::duration gracePeriod = std::chrono::hours{1};

  auto handle = m_frontPoolHandle->DBHandle();

  do {
    // this is why we prefer using nlohmann json wherever possible...
    // the bson API for constructing JSON documents is terrible. Unfortunately, to be safe, we prefer to
    // use the bson internal type for handling datetime objects in JSON, so that we are sure there are
    // no possible issues or strange mis-conversions when this data is handled by the DB.
    bsoncxx::document::value getPilotsQuery = bsoncxx::builder::basic::make_document(
        bsoncxx::builder::basic::kvp("lastHeartBeat", [](bsoncxx::builder::basic::sub_document sub_doc) {
          sub_doc.append(bsoncxx::builder::basic::kvp(
              "$gt", bsoncxx::types::b_date(std::chrono::system_clock::now() - gracePeriod)));
        }));

    auto queryResult = handle["pilots"].find(getPilotsQuery.view());
    for (const auto &_pilot : queryResult) {
      json pilot = JsonUtils::bson2json(_pilot);
      m_logger->debug("Removing dead pilot {}", pilot["uuid"]);

      json deleteQuery;
      deleteQuery["uuid"] = pilot["uuid"];

      handle["pilots"].delete_one(JsonUtils::json2bson(deleteQuery));

      json jobQuery;
      jobQuery["pilotUuid"] = pilot["uuid"];

      auto queryResult = handle["jobs"].find_one(JsonUtils::json2bson(jobQuery));
      if (queryResult) {
        json job = JsonUtils::bson2json(queryResult.value());
        auto result = handle.UpdateJobStatus(job["hash"].get<std::string_view>(), job["task"].get<std::string_view>(),
                                             JobStatus::Error);

        if (!result) {
          m_logger->warn("Couldn't update status for job {}, claimed by dead pilot {}", job["hash"], pilot["uuid"]);
        }
      }
    }

  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

void Director::DBSync() {
  static constexpr auto coolDown = std::chrono::seconds(60);

  static time_point lastCheck = std::chrono::system_clock::now();

  auto bHandle = m_backPoolHandle->DBHandle();
  auto fHandle = m_frontPoolHandle->DBHandle();

  do {
    // this is why we prefer using nlohmann json wherever possible...
    // the bson API for constructing JSON documents is terrible. Unfortunately, to be safe, we prefer to
    // use the bson internal type for handling datetime objects in JSON, so that we are sure there are
    // no possible issues or strange mis-conversions when this data is handled by the DB.
    bsoncxx::document::value getJobsQuery = bsoncxx::builder::basic::make_document(
        bsoncxx::builder::basic::kvp("lastUpdate", [](bsoncxx::builder::basic::sub_document sub_doc) {
          sub_doc.append(bsoncxx::builder::basic::kvp("$gt", bsoncxx::types::b_date(lastCheck)));
        }));

    auto queryResult = fHandle["jobs"].find(getJobsQuery.view());
    m_logger->debug("Syncing DBs...");

    unsigned int nUpdatedJobs{0};
    for (const auto &_job : queryResult) {
      json job = JsonUtils::bson2json(_job);

      json jobQuery;
      jobQuery["hash"] = job["hash"];

      json jobUpdateAction;
      jobUpdateAction["$set"]["status"] = job["status"];

      bHandle["jobs"].update_one(JsonUtils::json2bson(jobQuery), JsonUtils::json2bson(jobUpdateAction));
      nUpdatedJobs++;
    }

    lastCheck = std::chrono::system_clock::now();
    m_logger->debug("DBs synced: {} jobs updated in backend DB", nUpdatedJobs);
  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

bool Director::ValidateTaskToken(std::string_view task, std::string_view token) const {
  if (auto taskIt = m_tasks.find(std::string{task}); taskIt != end(m_tasks)) {
    return taskIt->second.token == token;
  }

  return false;
}

std::vector<std::string> Director::GetPilotTasks(std::string_view uuid) {
  auto handle = m_frontPoolHandle->DBHandle();
  std::vector<std::string> result;

  json query;
  query["uuid"] = uuid;
  auto query_result = handle["pilots"].find_one(JsonUtils::json2bson(query));
  if (query_result) {
    json dummy = JsonUtils::bson2json(query_result.value());
    std::copy(dummy["tasks"].begin(), dummy["tasks"].end(), std::back_inserter(result));
  }

  return result;
}

} // namespace PMS::Orchestrator