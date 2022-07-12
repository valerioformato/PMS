// c++ headers
#include <algorithm>

// external headers
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/ranges.h>
#include <mongocxx/bulk_write.hpp>
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
  m_threads.emplace_back(&Director::WriteJobUpdates, this);
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
    done &= (m_tasks[taskName].IsExhausted());
  }

  if (done)
    return R"({"finished": true})"_json;

  // NOTE(vformato): check which tasks are currently active, we'll only add those to the query
  std::vector<std::string_view> activeTasks;
  std::copy_if(begin(pilotInfo.tasks), end(pilotInfo.tasks), std::back_inserter(activeTasks),
               [this](const auto &taskName) { return m_tasks[taskName].IsActive(); });

  json filter;
  filter["status"]["$in"] =
      std::vector<std::string_view>{magic_enum::enum_name(JobStatus::Pending), magic_enum::enum_name(JobStatus::Error)};
  filter["task"]["$in"] = activeTasks;
  filter["tags"]["$all"] = pilotInfo.tags;

  json updateAction;
  updateAction["$set"]["status"] = magic_enum::enum_name(JobStatus::Claimed);
  updateAction["$set"]["pilotUuid"] = pilotUuid;
  updateAction["$inc"]["retries"] = 1;

  // NOTE(vformato): only keep fields that the pilot will actually need... This alleviates load on the DB
  json projectionOpt = R"({"_id":0, "dataset":0, "jobName":0, "status":0, "tags":0, "user":0})"_json;
  mongocxx::options::find_one_and_update query_options;
  query_options.bypass_document_validation(true).projection(JsonUtils::json2bson(projectionOpt));

  auto query_result = handle["jobs"].find_one_and_update(JsonUtils::json2bson(filter),
                                                         JsonUtils::json2bson(updateAction), query_options);

  if (query_result) {
    return JsonUtils::bson2json(query_result.value());
  } else {
    return R"({"sleep": true})"_json;
  }
}

Director::OperationResult Director::UpdateJobStatus(std::string_view pilotUuid, std::string_view hash,
                                                    std::string_view task, JobStatus status) {
  auto pilotTasks = GetPilotInfo(pilotUuid).tasks;
  if (std::find(begin(pilotTasks), end(pilotTasks), task) == end(pilotTasks))
    return OperationResult::DatabaseError;

  json jobFilter;
  jobFilter["task"] = task;
  jobFilter["hash"] = hash;

  json jobUpdateAction;
  jobUpdateAction["$set"]["status"] = magic_enum::enum_name(status);
  jobUpdateAction["$currentDate"]["lastUpdate"] = true;

  switch (status) {
  case JobStatus::Running:
    jobUpdateAction["$currentDate"]["startTime"] = true;
    break;
  case JobStatus::Error:
  case JobStatus::Done:
    jobUpdateAction["$currentDate"]["endTime"] = true;
    break;
  default:
    break;
  }

  {
    std::lock_guard lock{m_jobUpdateRequests_mx};
    m_jobUpdateRequests.push_back(
        mongocxx::model::update_one{JsonUtils::json2bson(jobFilter), JsonUtils::json2bson(jobUpdateAction)});
  }

  return OperationResult::Success;
}

struct WJU_PerfCounters {
  WJU_PerfCounters(std::chrono::system_clock::duration time_, unsigned int jobWrites_)
      : time{time_}, jobWrites{jobWrites_} {}

  std::chrono::system_clock::duration time;
  unsigned int jobWrites;
};

void Director::WriteJobUpdates() {
  static constexpr auto coolDown = std::chrono::seconds(1);
  static constexpr unsigned int nSamples = 60;
  static std::vector<WJU_PerfCounters> perfCounters;
  perfCounters.reserve(nSamples);

  auto handle = m_frontPoolHandle->DBHandle();
  do {
    if (!m_jobUpdateRequests.empty()) {
      auto start = std::chrono::system_clock::now();

      std::lock_guard lock{m_jobUpdateRequests_mx};
      handle["jobs"].bulk_write(m_jobUpdateRequests);

      auto end = std::chrono::system_clock::now();
      perfCounters.emplace_back(end - start, m_jobUpdateRequests.size());

      m_jobUpdateRequests.clear();

      // NOTE: This should be fine, right?
      json filter;
      filter["status"] = magic_enum::enum_name(JobStatus::Error);
      filter["retries"]["$gte"] = m_maxRetries;

      json updateAction;
      updateAction["$set"]["status"] = magic_enum::enum_name(JobStatus::Failed);
      handle["jobs"].update_many(JsonUtils::json2bson(filter), JsonUtils::json2bson(updateAction));
    }

    // do some performance logging
    if (perfCounters.size() == nSamples) {
      auto meanJobs = std::accumulate(begin(perfCounters), end(perfCounters), 0.0,
                                      [](const auto &curr, const auto &pfc) { return curr + pfc.jobWrites; }) /
                      nSamples;

      auto mean =
          std::accumulate(begin(perfCounters), end(perfCounters), 0.0,
                          [](const auto &curr, const auto &pfc) {
                            return curr + std::chrono::duration_cast<std::chrono::milliseconds>(pfc.time).count();
                          }) /
          nSamples;
      auto stdev =
          std::sqrt(std::accumulate(
              begin(perfCounters), end(perfCounters), 0.0,
              [mean](const auto &curr, const auto &pfc) {
                return curr + (std::chrono::duration_cast<std::chrono::milliseconds>(pfc.time).count() - mean) *
                                  (std::chrono::duration_cast<std::chrono::milliseconds>(pfc.time).count() - mean);
              })) /
          (nSamples - 1);
      m_logger->debug("[WriteJobUpdates] Wrote on average {:4.2f} jobs in {:4.2f} +- {:4.2f} ms", meanJobs, mean,
                      stdev);
      perfCounters.clear();
    }
  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

Director::NewPilotResult Director::RegisterNewPilot(std::string_view pilotUuid, std::string_view user,
                                                    const std::vector<std::pair<std::string, std::string>> &tasks,
                                                    const std::vector<std::string> &tags, const json &host_info) {
  NewPilotResult result{OperationResult::Success, {}, {}};

  auto handle = m_frontPoolHandle->DBHandle();

  json query;
  query["uuid"] = pilotUuid;
  query["user"] = user;
  query["tasks"] = json::array({});
  for (const auto &[taskName, token] : tasks) {
    if (ValidateTaskToken(taskName, token) == OperationResult::Success) {
      query["tasks"].push_back(taskName);
      result.validTasks.push_back(taskName);
    } else {
      result.invalidTasks.push_back(taskName);
    }
  }
  query["tags"] = tags;
  query["host"] = host_info;

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

  auto handle = m_backPoolHandle->DBHandle();

  json filter;
  filter["name"] = task;
  json updateAction;
  updateAction["$push"]["dependencies"] = dependsOn;
  handle["tasks"].update_one(JsonUtils::json2bson(filter), JsonUtils::json2bson(updateAction));

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
  static constexpr auto coolDown = std::chrono::milliseconds(10);

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
      m_logger->trace("Inserting {} new jobs into backend DB", toBeInserted.size());
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
  std::vector<mongocxx::model::write> writeOps;

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

      std::transform(begin(toBeInserted), end(toBeInserted), std::back_inserter(writeOps), [](const auto &jobIt) {
        json job = JsonUtils::bson2json(jobIt);

        json updateFilter;
        updateFilter["hash"] = job["hash"];

        json updateAction;
        updateAction["$set"]["inFrontDB"] = true;

        return mongocxx::model::update_one{JsonUtils::json2bson(updateFilter), JsonUtils::json2bson(updateAction)};
      });

      bHandle["jobs"].bulk_write(writeOps);

      toBeInserted.clear();
      writeOps.clear();
    }
  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

void Director::UpdateTasks() {
  static constexpr auto coolDown = std::chrono::seconds(60);

  auto backHandle = m_backPoolHandle->DBHandle();
  auto frontHandle = m_frontPoolHandle->DBHandle();

  do {
    m_logger->debug("Updating tasks");

    // get list of all tasks
    auto tasksResult = backHandle["tasks"].find({});

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
        std::copy(tmpdoc["dependencies"].begin(), tmpdoc["dependencies"].end(), std::back_inserter(task.dependencies));
      }

      // skip failed tasks, since failed jobs won't be tried anymore
      if (task.IsFailed()) {
        continue;
      }

      auto markTaskAsFailed = [&frontHandle](const std::string_view taskName) {
        json filter;
        filter["task"] = taskName;

        json updateQuery;
        updateQuery["$set"]["status"] = magic_enum::enum_name(JobStatus::Failed);
        updateQuery["$currentDate"]["lastUpdate"] = true;

        frontHandle["jobs"].update_many(JsonUtils::json2bson(filter), JsonUtils::json2bson(updateQuery));
      };

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

          if (requiredTaskIt->second.IsFailed()) {
            markTaskAsFailed(task.name);
            continue;
          }

          taskIsReady &= requiredTaskIt->second.IsFinished();
        }

        task.readyForScheduling = taskIsReady;
      }

      // update job counters in task
      json countQuery;
      countQuery["task"] = taskName;
      task.totJobs = backHandle["jobs"].count_documents(JsonUtils::json2bson(countQuery));

      std::vector<std::string> statusSummary;
      for (auto status : magic_enum::enum_values<JobStatus>()) {
        countQuery["status"] = magic_enum::enum_name(status);
        task.jobs[status] = backHandle["jobs"].count_documents(JsonUtils::json2bson(countQuery));
        statusSummary.push_back(fmt::format("{} {}", task.jobs[status], magic_enum::enum_name(status)));
      }

      m_logger->debug("Task {} updated - {} job{} ({}) - status: {}{}{}", task.name, task.totJobs,
                      task.totJobs > 1 ? "s" : "", fmt::join(statusSummary, ", "), task.IsActive() ? "A" : "",
                      task.IsExhausted() ? "E" : "", task.IsFinished() ? "F" : "");
    }

  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

using time_point = std::chrono::system_clock::time_point;

void Director::UpdatePilots() {
  static constexpr auto coolDown = std::chrono::seconds(60);

  static constexpr std::chrono::system_clock::duration gracePeriod = std::chrono::hours{1};

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

      json jobQuery;
      jobQuery["pilotUuid"] = pilot["uuid"];
      jobQuery["status"] = magic_enum::enum_name(JobStatus::Running);

      auto queryJResult = handle["jobs"].find_one(JsonUtils::json2bson(jobQuery));
      if (queryJResult) {
        m_logger->debug("Dead pilot {} had a running job, setting to Error...", pilot["uuid"]);
        json job = JsonUtils::bson2json(queryJResult.value());
        UpdateJobStatus(pilot["uuid"].get<std::string_view>(), job["hash"].get<std::string_view>(),
                        job["task"].get<std::string_view>(), JobStatus::Error);
      }

      handle["pilots"].delete_one(JsonUtils::json2bson(deleteQuery));

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

    m_logger->debug("Syncing DBs...");
    auto queryResult = fHandle["jobs"].find(getJobsQuery.view());
    lastCheck = std::chrono::system_clock::now();
    m_logger->debug("...query done.");

    std::vector<mongocxx::model::write> writeOps;
    std::transform(queryResult.begin(), queryResult.end(), std::back_inserter(writeOps), [](const auto &_job) {
      json job = JsonUtils::bson2json(_job);

      json jobQuery;
      jobQuery["hash"] = job["hash"];

      json jobUpdateAction;
      jobUpdateAction["$set"]["status"] = job["status"];

      if (auto status = magic_enum::enum_cast<JobStatus>(job["status"].get<std::string_view>()); status.has_value()) {
        if (status.value() == JobStatus::Running) {
          jobUpdateAction["$set"]["startTime"] = job["startTime"];
          jobUpdateAction["$set"]["pilotUuid"] = job["pilotUuid"];
        }
        if (status.value() == JobStatus::Done || status.value() == JobStatus::Error) {
          jobUpdateAction["$set"]["endTime"] = job["endTime"];
        }
      }

      return mongocxx::model::update_one{JsonUtils::json2bson(jobQuery), JsonUtils::json2bson(jobUpdateAction)};
    });
    if (!writeOps.empty())
      bHandle["jobs"].bulk_write(writeOps);

    m_logger->debug("DBs synced: {} jobs updated in backend DB", writeOps.size());
  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

Director::OperationResult Director::ValidateTaskToken(std::string_view task, std::string_view token) const {
  if (auto taskIt = m_tasks.find(std::string{task}); taskIt != end(m_tasks)) {
    return taskIt->second.token == token ? OperationResult::Success : OperationResult::ProcessError;
  }

  return OperationResult::DatabaseError;
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

std::string Director::Summary(const std::string &user) const {
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

    Task task = m_tasks.at(taskName);

    json taskSummary;
    taskSummary["taskname"] = task.name;
    for (auto status : magic_enum::enum_values<JobStatus>()) {
      taskSummary[magic_enum::enum_name(status).data()] = task.jobs[status];
    }

    summary.push_back(taskSummary);
  }

  return summary.dump();
}

std::string Director::QueryBackDB(const json &match, const json &filter) const {
  auto handle = m_backPoolHandle->DBHandle();

  json projectionOpt;
  if (filter.empty()) {
    projectionOpt = R"({"_id":0})"_json;
  } else {
    projectionOpt = filter;
    projectionOpt["_id"] = 0;
  }
  mongocxx::options::find query_options;
  query_options.projection(JsonUtils::json2bson(projectionOpt));

  json resp;
  resp["result"] = json::array({});

  auto query_result = handle["jobs"].find(JsonUtils::json2bson(match), query_options);
  std::transform(query_result.begin(), query_result.end(), std::back_inserter(resp["result"]),
                 [](const auto &job) { return JsonUtils::bson2json(job); });

  return resp.dump();
}

} // namespace PMS::Orchestrator
