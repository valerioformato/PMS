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
  m_frontPoolHandle->DBHandle().SetupDBCollections();

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

  auto pilotInfo = GetPilotInfo(pilotUuid);
  bool done = true;
  for (const auto &taskName : pilotInfo.tasks) {
    const auto &task = m_tasks[taskName];
    done &= !task.IsExhausted();
  }
  if (done)
    return R"({"finished": true})"_json;

  json filter;
  filter["status"] = magic_enum::enum_name(JobStatus::Pending);
  filter["task"]["$in"] = pilotInfo.tasks;
  filter["tags"]["$all"] = pilotInfo.tags;

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

  auto pilotTasks = GetPilotInfo(pilotUuid).tasks;
  if (std::find(begin(pilotTasks), end(pilotTasks), task) == end(pilotTasks))
    return OperationResult::DatabaseError;

  return handle.UpdateJobStatus(hash, task, status) ? OperationResult::Success : OperationResult::DatabaseError;
}

Director::NewPilotResult Director::RegisterNewPilot(std::string_view pilotUuid, std::string_view user,
                                                    const std::vector<std::pair<std::string, std::string>> &tasks,
                                                    const std::vector<std::string> &tags) {
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
  query["tags"] = tags;

  m_activePilots[std::string(pilotUuid)] = PilotInfo{result.validTasks, tags};

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

  if (auto pilotIt = m_activePilots.find(std::string{pilotUuid}); pilotIt != end(m_activePilots))
    m_activePilots.erase(pilotIt);

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

Director::OperationResult Director::ClearTask(const std::string &task, bool deleteTask) {
  auto bHandle = m_backPoolHandle->DBHandle();
  auto fHandle = m_frontPoolHandle->DBHandle();

  try {
    json jobDeleteQuery;
    jobDeleteQuery["task"] = task;

    m_logger->debug("Cleaning task {}", task);
    bHandle["jobs"].delete_many(JsonUtils::json2bson(jobDeleteQuery));
    fHandle["jobs"].delete_many(JsonUtils::json2bson(jobDeleteQuery));

    if (deleteTask) {
      json taskDeleteQuery;
      taskDeleteQuery["name"] = task;

      bHandle["tasks"].delete_many(JsonUtils::json2bson(taskDeleteQuery));
      m_tasks.erase(task);
      m_logger->debug("Task {} deleted", task);
    }
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

      for (auto status : magic_enum::enum_values<JobStatus>()) {
        countQuery["status"] = magic_enum::enum_name(status);
        task.jobs[status] = handle["jobs"].count_documents(JsonUtils::json2bson(countQuery));
      }

      m_logger->debug("Task {} updated - {} job{} ({} done, {} failed, {} running, {} claimed, {} pending)", task.name,
                      task.totJobs, task.totJobs > 1 ? "s" : "", task.jobs[JobStatus::Done],
                      task.jobs[JobStatus::Error], task.jobs[JobStatus::Running], task.jobs[JobStatus::Claimed],
                      task.jobs[JobStatus::Pending]);
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
              "$lt", bsoncxx::types::b_date(std::chrono::system_clock::now() - gracePeriod)));
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
      jobQuery["status"] = magic_enum::enum_name(JobStatus::Running);

      auto queryJResult = handle["jobs"].find_one(JsonUtils::json2bson(jobQuery));
      if (queryJResult) {
        json job = JsonUtils::bson2json(queryJResult.value());
        auto result = handle.UpdateJobStatus(job["hash"].get<std::string_view>(), job["task"].get<std::string_view>(),
                                             JobStatus::Error);

        if (!result) {
          m_logger->warn("Couldn't update status for job {}, claimed by dead pilot {}", job["hash"], pilot["uuid"]);
        }
      }

      if (auto pilotIt = m_activePilots.find(pilot["uuid"]); pilotIt != end(m_activePilots))
        m_activePilots.erase(pilotIt);
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

Director::PilotInfo Director::GetPilotInfo(std::string_view uuid) {
  if (auto uuidString = std::string{uuid}; m_activePilots.find(uuidString) != end(m_activePilots)) {
    return m_activePilots[uuidString];
  } else {
    auto handle = m_frontPoolHandle->DBHandle();
    PilotInfo result;

    json query;
    query["uuid"] = uuid;

    if (auto query_result = handle["pilots"].find_one(JsonUtils::json2bson(query)); query_result) {
      json dummy = JsonUtils::bson2json(query_result.value());
      std::copy(dummy["tasks"].begin(), dummy["tasks"].end(), std::back_inserter(result.tasks));
      std::copy(dummy["tags"].begin(), dummy["tags"].end(), std::back_inserter(result.tags));
    }

    m_activePilots[uuidString] = result;

    return result;
  }
}

std::string Director::Summary(const std::string &user) {
  auto handle = m_frontPoolHandle->DBHandle();

  json summary = json::array({});

  mongocxx::pipeline aggregationPipeline;

  json matchingArgs;
  matchingArgs["user"] = user;
  aggregationPipeline.match(JsonUtils::json2bson(matchingArgs));

  json groupingArgs;
  groupingArgs["_id"] = "$task";
  aggregationPipeline.group(JsonUtils::json2bson(groupingArgs));

  auto tasksQueryResult = handle["jobs"].aggregate(aggregationPipeline);
  for (auto item : tasksQueryResult) {
    json result = JsonUtils::bson2json(item);
    auto taskName = result["_id"];

    if (m_tasks.find(taskName) == end(m_tasks)) {
      continue;
    }

    Task task = m_tasks[taskName];

    json taskSummary;
    taskSummary["taskname"] = task.name;
    for (auto status : magic_enum::enum_values<JobStatus>()) {
      taskSummary[magic_enum::enum_name(status).data()] = task.jobs[status];
    }

    summary.push_back(taskSummary);
  }

  return summary.dump();
}

} // namespace PMS::Orchestrator