// c++ headers
#include <algorithm>

// external headers
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/ranges.h>
#include <mongocxx/bulk_write.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/pipeline.hpp>
#include <nlohmann/json.hpp>

// our headers
#include "common/Job.h"
#include "common/JsonUtils.h"
#include "orchestrator/Director.h"

using json = nlohmann::json;

namespace PMS::Orchestrator {

// FIXME: find a better solution for decorating DB queries. We don't want exceptions bringing the server down
template <class ErrorType, unsigned int max_retries = 10, typename Callable>
decltype(auto) RetryIfFailsWith(Callable callable) {
  unsigned int retries = 0;
  std::exception_ptr last_exception;

  while (retries < max_retries) {
    try {
      return callable();
    } catch (const ErrorType &e) {
      spdlog::trace("DB query failed, retrying... {}/{}", retries, max_retries);
      ++retries;
      last_exception = std::current_exception();
    }
  }

  // if we got here it's because we failed too many times. Propagate last error up the stack frame.
  std::rethrow_exception(last_exception);
}

void Director::Start() {
  m_backPoolHandle->DBHandle().SetupDBCollections();
  m_frontPoolHandle->DBHandle().SetupDBCollections();

  m_threads.emplace_back(&Director::UpdateDeadPilots, this);
  m_threads.emplace_back(&Director::UpdateTasks, this);
  m_threads.emplace_back(&Director::JobInsert, this);
  m_threads.emplace_back(&Director::JobTransfer, this);
  m_threads.emplace_back(&Director::DBSync, this);
  m_threads.emplace_back(&Director::WriteJobUpdates, this);
  m_threads.emplace_back(&Director::WriteHeartBeatUpdates, this);
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

  auto maybe_pilotInfo = GetPilotInfo(pilotUuid);
  if (!maybe_pilotInfo.has_value()) {
    return R"({"error": "unknown pilot"})"_json;
  }

  const auto &pilotInfo = maybe_pilotInfo.value();

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
  if (pilotInfo.tags.empty()) {
    filter["tags"]["$type"] = "array";
    filter["tags"]["$eq"] = std::vector<std::string>{};
  } else
    filter["tags"]["$all"] = pilotInfo.tags;

  json updateAction;
  updateAction["$set"]["status"] = magic_enum::enum_name(JobStatus::Claimed);
  updateAction["$set"]["pilotUuid"] = pilotUuid;
  updateAction["$inc"]["retries"] = 1;

  // NOTE(vformato): only keep fields that the pilot will actually need... This alleviates load on the DB
  json projectionOpt = R"({"_id":0, "dataset":0, "jobName":0, "status":0, "tags":0, "user":0})"_json;
  mongocxx::options::find_one_and_update query_options;
  query_options.bypass_document_validation(true).projection(JsonUtils::json2bson(projectionOpt));

  auto query_result = RetryIfFailsWith<mongocxx::exception>([&]() {
    return handle["jobs"].find_one_and_update(JsonUtils::json2bson(filter), JsonUtils::json2bson(updateAction),
                                              query_options);
  });

  if (query_result) {
    return JsonUtils::bson2json(query_result.value());
  } else {
    return R"({"sleep": true})"_json;
  }
}

Director::OperationResult Director::UpdateJobStatus(std::string_view pilotUuid, std::string_view hash,
                                                    std::string_view task, JobStatus status) {
  auto maybe_pilotInfo = GetPilotInfo(pilotUuid);
  if (!maybe_pilotInfo.has_value()) {
    return OperationResult::DatabaseError;
  }

  const auto &pilotInfo = maybe_pilotInfo.value();

  auto pilotTasks = pilotInfo.tasks;
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
    [[maybe_unused]] std::lock_guard lock{m_jobUpdateRequests_mx};
    m_jobUpdateRequests.push_back(
        mongocxx::model::update_one{JsonUtils::json2bson(jobFilter), JsonUtils::json2bson(jobUpdateAction)});
  }

  return OperationResult::Success;
}

// TODO: This has never been really needed in a long time. Consider removing it.
// struct WJU_PerfCounters {
//  WJU_PerfCounters(std::chrono::system_clock::duration time_, unsigned int jobWrites_)
//      : time{time_}, jobWrites{jobWrites_} {}
//
//  std::chrono::system_clock::duration time;
//  unsigned int jobWrites;
//};

void Director::WriteJobUpdates() {
  static constexpr auto coolDown = std::chrono::seconds(1);

  auto handle = m_frontPoolHandle->DBHandle();
  do {
    if (!m_jobUpdateRequests.empty()) {
      std::vector<mongocxx::model::write> job_update_requests{};

      {
        std::lock_guard lock{m_jobUpdateRequests_mx};
        std::swap(m_jobUpdateRequests, job_update_requests);
      }
      RetryIfFailsWith<mongocxx::exception>([&]() { handle["jobs"].bulk_write(job_update_requests); });

      json filter;
      filter["status"]["$in"] = std::vector<std::string_view>{magic_enum::enum_name(JobStatus::Error),
                                                              magic_enum::enum_name(JobStatus::InboundTransferError),
                                                              magic_enum::enum_name(JobStatus::OutboundTransferError)};
      filter["retries"]["$gte"] = m_maxRetries;

      json updateAction;
      updateAction["$set"]["status"] = magic_enum::enum_name(JobStatus::Failed);

      RetryIfFailsWith<mongocxx::exception>(
          [&]() { handle["jobs"].update_many(JsonUtils::json2bson(filter), JsonUtils::json2bson(updateAction)); });
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

  auto query_result =
      RetryIfFailsWith<mongocxx::exception>([&]() { return handle["pilots"].insert_one(JsonUtils::json2bson(query)); });
  return bool(query_result) ? result : NewPilotResult{OperationResult::DatabaseError, {}, {}};
}

Director::OperationResult Director::UpdateHeartBeat(std::string_view pilotUuid) {
  m_heartbeatUpdates.emplace(std::string{pilotUuid}, std::chrono::system_clock::now());
  return Director::OperationResult::Success;
}

void Director::WriteHeartBeatUpdates() {
  static constexpr auto coolDown = std::chrono::minutes(1);

  do {
    auto handle = m_frontPoolHandle->DBHandle();
    std::unordered_map<std::string, time_point> unique_hbs;
    auto hbs = m_heartbeatUpdates.consume_all();
    // NOTE: de-duplicate heartbeats. If all is correct, requests are ordered in time, so only most recent one survives
    std::for_each(begin(hbs), end(hbs), [&unique_hbs](const PilotHeartBeat &hb) { unique_hbs[hb.uuid] = hb.time; });

    std::vector<mongocxx::model::write> requests;
    for (const auto &[uuid, time] : unique_hbs) {
      // NOTE: if this pilot is not in the active cache *and* not in the front DB collection then it's either an unknown
      // pilot or it sent an update before dying and being removed from both cache and DB
      if (auto maybe_pilotInfo = GetPilotInfo(uuid); !maybe_pilotInfo.has_value())
        continue;

      json updateFilter;
      updateFilter["uuid"] = uuid;

      // this is why we prefer using nlohmann json wherever possible...
      // the bson API for constructing JSON documents is terrible. Unfortunately, to be safe, we prefer to
      // use the bson internal type for handling datetime objects in JSON, so that we are sure there are
      // no possible issues or strange mis-conversions when this data is handled by the DB.
      bsoncxx::view_or_value<bsoncxx::document::view, bsoncxx::document::value> updateAction =
          bsoncxx::builder::basic::make_document(
              bsoncxx::builder::basic::kvp("$set", [tp = time](bsoncxx::builder::basic::sub_document sub_doc) {
                sub_doc.append(bsoncxx::builder::basic::kvp("lastHeartBeat", bsoncxx::types::b_date(tp)));
              }));

      requests.push_back(mongocxx::model::update_one{JsonUtils::json2bson(updateFilter), updateAction}.upsert(true));
    }

    if (!requests.empty()) {
      m_logger->debug("Updating {} heartbeats", requests.size());
      mongocxx::options::bulk_write write_options;
      write_options.ordered(false);
      RetryIfFailsWith<mongocxx::exception>([&]() { handle["pilots"].bulk_write(requests, write_options); });
    }
  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

Director::OperationResult Director::DeleteHeartBeat(std::string_view pilotUuid) {
  auto handle = m_frontPoolHandle->DBHandle();

  if (auto pilotIt = m_activePilots.find(std::string{pilotUuid}); pilotIt != end(m_activePilots))
    m_activePilots.erase(pilotIt);

  json deleteFilter;
  deleteFilter["uuid"] = pilotUuid;

  auto queryResult = RetryIfFailsWith<mongocxx::exception>(
      [&]() { return handle["pilots"].delete_one(JsonUtils::json2bson(deleteFilter)); });

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
  RetryIfFailsWith<mongocxx::exception>(
      [&]() { handle["tasks"].update_one(JsonUtils::json2bson(filter), JsonUtils::json2bson(updateAction)); });

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

  RetryIfFailsWith<mongocxx::exception>([&]() { handle["tasks"].insert_one(JsonUtils::json2bson(insertQuery)); });

  return {OperationResult::Success, token};
}

Director::OperationResult Director::ClearTask(const std::string &task, bool deleteTask) {
  auto bHandle = m_backPoolHandle->DBHandle();
  auto fHandle = m_frontPoolHandle->DBHandle();

  try {
    json jobDeleteQuery;
    jobDeleteQuery["task"] = task;

    m_logger->debug("Cleaning task {}", task);
    RetryIfFailsWith<mongocxx::exception>([&]() { bHandle["jobs"].delete_many(JsonUtils::json2bson(jobDeleteQuery)); });
    RetryIfFailsWith<mongocxx::exception>([&]() { fHandle["jobs"].delete_many(JsonUtils::json2bson(jobDeleteQuery)); });

    if (deleteTask) {
      json taskDeleteQuery;
      taskDeleteQuery["name"] = task;

      RetryIfFailsWith<mongocxx::exception>(
          [&]() { bHandle["tasks"].delete_many(JsonUtils::json2bson(taskDeleteQuery)); });
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

      if (auto queryResult = RetryIfFailsWith<mongocxx::exception>(
              [&]() { return handle["jobs"].find_one(JsonUtils::json2bson(jobQuery)); });
          queryResult)
        continue;

      m_logger->trace("Queueing up job {} for insertion", job["hash"]);

      // job initial status should always be Pending :)
      job["status"] = magic_enum::enum_name(JobStatus::Pending);
      toBeInserted.push_back(JsonUtils::json2bson(job));
    }

    if (!toBeInserted.empty()) {
      m_logger->trace("Inserting {} new jobs into backend DB", toBeInserted.size());
      RetryIfFailsWith<mongocxx::exception>([&]() { handle["jobs"].insert_many(toBeInserted); });

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

      auto queryResult = RetryIfFailsWith<mongocxx::exception>(
          [&]() { return bHandle["jobs"].find(JsonUtils::json2bson(getJobsQuery)); });
      for (const auto &job : queryResult) {
        // filtering out the _id field
        toBeInserted.push_back(
            JsonUtils::filter(job, [](const bsoncxx::document::element &el) { return el.key() == "_id"; }));
      }
    }

    if (!toBeInserted.empty()) {
      m_logger->debug("Inserting {} new jobs into frontend DB", toBeInserted.size());
      RetryIfFailsWith<mongocxx::exception>([&]() { fHandle["jobs"].insert_many(toBeInserted); });

      std::transform(begin(toBeInserted), end(toBeInserted), std::back_inserter(writeOps), [](const auto &jobIt) {
        json job = JsonUtils::bson2json(jobIt);

        json updateFilter;
        updateFilter["hash"] = job["hash"];

        json updateAction;
        updateAction["$set"]["inFrontDB"] = true;

        return mongocxx::model::update_one{JsonUtils::json2bson(updateFilter), JsonUtils::json2bson(updateAction)};
      });

      RetryIfFailsWith<mongocxx::exception>([&]() { bHandle["jobs"].bulk_write(writeOps); });

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
    auto tasksResult = RetryIfFailsWith<mongocxx::exception>([&]() { return backHandle["tasks"].find({}); });

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

      // skip stale failed tasks, since failed jobs won't be tried anymore
      if (!task.IsActive() && task.IsFailed()) {
        continue;
      }

      auto markTaskAsFailed = [&frontHandle](const std::string_view taskName) {
        json filter;
        filter["task"] = taskName;

        json updateQuery;
        updateQuery["$set"]["status"] = magic_enum::enum_name(JobStatus::Failed);
        updateQuery["$currentDate"]["lastUpdate"] = true;

        RetryIfFailsWith<mongocxx::exception>([&]() {
          frontHandle["jobs"].update_many(JsonUtils::json2bson(filter), JsonUtils::json2bson(updateQuery));
        });
      };

      if (task.dependencies.empty()) {
        task.readyForScheduling = true;
      } else if (!task.IsFailed()) {
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

      UpdateTaskCounts(task);
      std::vector<std::string> statusSummary;
      for (auto status : magic_enum::enum_values<JobStatus>()) {
        statusSummary.push_back(fmt::format("{} {}", task.jobs[status], magic_enum::enum_name(status)));
      }

      m_logger->debug("Task {} updated - {} job{} ({}) - status: {}{}{}{}", task.name, task.totJobs,
                      task.totJobs > 1 ? "s" : "", fmt::join(statusSummary, ", "), task.IsActive() ? "A" : "",
                      task.IsExhausted() ? "E" : "", task.IsFinished() ? "F" : "", task.IsFailed() ? "X" : "");
    }

  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

void Director::UpdateTaskCounts(Task &task) { // update job counters in task
  auto backHandle = m_backPoolHandle->DBHandle();

  json countQuery;
  countQuery["task"] = task.name;
  task.totJobs = RetryIfFailsWith<mongocxx::exception>(
      [&]() { return backHandle["jobs"].count_documents(JsonUtils::json2bson(countQuery)); });

  for (auto status : magic_enum::enum_values<JobStatus>()) {
    countQuery["status"] = magic_enum::enum_name(status);
    task.jobs[status] = RetryIfFailsWith<mongocxx::exception>(
        [&]() { return backHandle["jobs"].count_documents(JsonUtils::json2bson(countQuery)); });
  }
}

void Director::UpdateDeadPilots() {
  static constexpr auto coolDown = std::chrono::minutes(10);

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

    auto queryResult =
        RetryIfFailsWith<mongocxx::exception>([&]() { return handle["pilots"].find(getPilotsQuery.view()); });
    std::vector<mongocxx::model::write> requests;
    for (const auto &_pilot : queryResult) {
      json pilot = JsonUtils::bson2json(_pilot);
      m_logger->debug("Removing dead pilot {}", pilot["uuid"]);

      json deleteQuery;
      deleteQuery["uuid"] = pilot["uuid"];

      json jobQuery;
      jobQuery["pilotUuid"] = pilot["uuid"];
      jobQuery["status"]["$in"] = std::vector<std::string_view>{magic_enum::enum_name(JobStatus::Running),
                                                                magic_enum::enum_name(JobStatus::InboundTransfer),
                                                                magic_enum::enum_name(JobStatus::OutboundTransfer)};

      json projectionOpt = R"({"_id":1, "hash":1, "task":1})"_json;
      mongocxx::options::find query_options;
      query_options.projection(JsonUtils::json2bson(projectionOpt));

      auto queryJResult = RetryIfFailsWith<mongocxx::exception>(
          [&]() { return handle["jobs"].find_one(JsonUtils::json2bson(jobQuery), query_options); });
      if (queryJResult) {
        json job = JsonUtils::bson2json(queryJResult.value());
        m_logger->debug("Dead pilot {} had a running job ({}), setting to Error...", pilot["uuid"],
                        job["hash"].get<std::string_view>());
        UpdateJobStatus(pilot["uuid"].get<std::string_view>(), job["hash"].get<std::string_view>(),
                        job["task"].get<std::string_view>(), JobStatus::Error);
      }

      requests.push_back(mongocxx::model::delete_one(JsonUtils::json2bson(deleteQuery)));
      // RetryIfFailsWith<mongocxx::exception>([&]() { handle["pilots"].delete_one(JsonUtils::json2bson(deleteQuery));
      // });

      if (auto pilotIt = m_activePilots.find(pilot["uuid"]); pilotIt != end(m_activePilots))
        m_activePilots.erase(pilotIt);
    }

    if (!requests.empty()) {
      m_logger->debug("Deleting {} pilots from front DB", requests.size());
      RetryIfFailsWith<mongocxx::exception>([&]() { handle["pilots"].bulk_write(requests); });
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
    auto queryResult =
        RetryIfFailsWith<mongocxx::exception>([&]() { return fHandle["jobs"].find(getJobsQuery.view()); });
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
      RetryIfFailsWith<mongocxx::exception>([&]() { bHandle["jobs"].bulk_write(writeOps); });

    m_logger->debug("DBs synced: {} jobs updated in backend DB", writeOps.size());
  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

Director::OperationResult Director::ValidateTaskToken(std::string_view task, std::string_view token) const {
  if (auto taskIt = m_tasks.find(std::string{task}); taskIt != end(m_tasks)) {
    return taskIt->second.token == token ? OperationResult::Success : OperationResult::ProcessError;
  }

  return OperationResult::DatabaseError;
}

std::optional<Director::PilotInfo> Director::GetPilotInfo(std::string_view uuid) {
  if (auto uuidString = std::string{uuid}; m_activePilots.find(uuidString) != end(m_activePilots)) {
    return m_activePilots[uuidString];
  } else {
    auto handle = m_frontPoolHandle->DBHandle();
    PilotInfo result;

    json query;
    query["uuid"] = uuid;

    if (auto query_result = RetryIfFailsWith<mongocxx::exception>(
            [&]() { return handle["pilots"].find_one(JsonUtils::json2bson(query)); });
        query_result) {
      json dummy = JsonUtils::bson2json(query_result.value());
      std::copy(dummy["tasks"].begin(), dummy["tasks"].end(), std::back_inserter(result.tasks));
      std::copy(dummy["tags"].begin(), dummy["tags"].end(), std::back_inserter(result.tags));
      m_activePilots[uuidString] = result;
      return result;
    }

    return {};
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

  auto tasksQueryResult =
      RetryIfFailsWith<mongocxx::exception>([&]() { return handle["jobs"].aggregate(aggregationPipeline); });
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

Director::QueryResult Director::QueryBackDB(QueryOperation operation, const json &match, const json &option) const {
  auto backHandle = m_backPoolHandle->DBHandle();
  auto frontHandle = m_frontPoolHandle->DBHandle();

  m_logger->debug("QueryBackDB: {} {} {}", magic_enum::enum_name(operation), match.dump(), option.dump());

  switch (operation) {
  case QueryOperation::UpdateOne: {
    auto jobUpdateAction = option;
    jobUpdateAction["$currentDate"]["lastUpdate"] = true;
    auto query_result = RetryIfFailsWith<mongocxx::exception>([&]() {
      return frontHandle["jobs"].update_one(JsonUtils::json2bson(match), JsonUtils::json2bson(jobUpdateAction));
    });
    if (query_result)
      return {OperationResult::Success, fmt::format("Updated job {}", match["hash"])};
    else {
      return {OperationResult::DatabaseError, fmt::format("Could not update job {}", match["hash"])};
    }
  }
  case QueryOperation::UpdateMany: {
    auto jobUpdateAction = option;
    jobUpdateAction["$currentDate"]["lastUpdate"] = true;
    auto query_result = RetryIfFailsWith<mongocxx::exception>([&]() {
      return frontHandle["jobs"].update_many(JsonUtils::json2bson(match), JsonUtils::json2bson(jobUpdateAction));
    });
    if (query_result)
      return {OperationResult::Success, fmt::format("Matched {} jobs. Updated {} jobs", query_result->matched_count(),
                                                    query_result->modified_count())};
    else {
      return {OperationResult::DatabaseError, "Could not update jobs"};
    }
  }
  case QueryOperation::DeleteOne: {
    auto front_query_result = RetryIfFailsWith<mongocxx::exception>(
        [&]() { return frontHandle["jobs"].delete_one(JsonUtils::json2bson(match)); });
    auto back_query_result = RetryIfFailsWith<mongocxx::exception>(
        [&]() { return backHandle["jobs"].delete_one(JsonUtils::json2bson(match)); });
    if (front_query_result && back_query_result)
      return {OperationResult::Success, fmt::format("Deleted job {}", match["hash"])};
    else {
      return {OperationResult::DatabaseError, fmt::format("Could not update job {}", match["hash"])};
    }
  }
  case QueryOperation::DeleteMany: {
    auto back_query_result = RetryIfFailsWith<mongocxx::exception>(
        [&]() { return backHandle["jobs"].delete_many(JsonUtils::json2bson(match)); });
    auto front_query_result = RetryIfFailsWith<mongocxx::exception>(
        [&]() { return frontHandle["jobs"].delete_many(JsonUtils::json2bson(match)); });
    if (front_query_result && back_query_result)
      return {OperationResult::Success, fmt::format("Deleted {} jobs.", back_query_result->deleted_count())};
    else {
      return {OperationResult::DatabaseError, "Could not delete jobs"};
    }
  }
  case QueryOperation::Find: {
    json projectionOpt;
    if (option.empty()) {
      projectionOpt = R"({"_id":0})"_json;
    } else {
      projectionOpt = option;
      projectionOpt["_id"] = 0;
    }
    mongocxx::options::find query_options;
    query_options.projection(JsonUtils::json2bson(projectionOpt));

    json resp;
    resp["result"] = json::array({});

    auto query_result = RetryIfFailsWith<mongocxx::exception>(
        [&]() { return backHandle["jobs"].find(JsonUtils::json2bson(match), query_options); });
    std::transform(query_result.begin(), query_result.end(), std::back_inserter(resp["result"]),
                   [](const auto &job) { return JsonUtils::bson2json(job); });

    return {OperationResult::Success, resp.dump()};
  }
  }

  return {};
}

Director::QueryResult Director::QueryFrontDB(DBCollection collection, const json &match, const json &filter) const {
  auto handle = m_frontPoolHandle->DBHandle();

  std::string_view collection_name;
  switch (collection) {
  case DBCollection::Pilots:
    collection_name = "pilots";
    break;
  case DBCollection::Jobs:
    collection_name = "jobs";
    break;
  }

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

  auto query_result = RetryIfFailsWith<mongocxx::exception>(
      [&]() { return handle[collection_name.data()].find(JsonUtils::json2bson(match), query_options); });
  std::transform(query_result.begin(), query_result.end(), std::back_inserter(resp["result"]),
                 [](const auto &job) { return JsonUtils::bson2json(job); });

  return {OperationResult::Success, resp.dump()};
}

Director::OperationResult Director::ResetFailedJobs(std::string_view taskname) {
  json filter;
  filter["task"] = taskname;
  filter["status"] = magic_enum::enum_name(JobStatus::Failed);

  json updateAction;
  updateAction["$set"]["status"] = magic_enum::enum_name(JobStatus::Pending);
  updateAction["$set"]["retries"] = 0;

  try {
    auto front_handle = m_frontPoolHandle->DBHandle();
    auto n_docs = RetryIfFailsWith<mongocxx::exception>(
        [&]() { return front_handle["jobs"].count_documents(JsonUtils::json2bson(filter)); });
    m_logger->debug("ResetFailedJobs: Found {} documents in front DB to update", n_docs);
    RetryIfFailsWith<mongocxx::exception>(
        [&]() { front_handle["jobs"].update_many(JsonUtils::json2bson(filter), JsonUtils::json2bson(updateAction)); });

    auto back_handle = m_backPoolHandle->DBHandle();
    n_docs = RetryIfFailsWith<mongocxx::exception>(
        [&]() { return back_handle["jobs"].count_documents(JsonUtils::json2bson(filter)); });
    m_logger->debug("ResetFailedJobs: Found {} documents in back DB to update", n_docs);
    RetryIfFailsWith<mongocxx::exception>(
        [&]() { back_handle["jobs"].update_many(JsonUtils::json2bson(filter), JsonUtils::json2bson(updateAction)); });

    m_logger->debug("Reset all failed jobs in taskname {}", taskname);
  } catch (const std::exception &e) {
    m_logger->error("Server query failed with error {}", e.what());
    return OperationResult::DatabaseError;
  }

  std::string s_taskname{taskname};
  UpdateTaskCounts(m_tasks.at(s_taskname));

  return Director::OperationResult::Success;
}

} // namespace PMS::Orchestrator
