// c++ headers
#include <algorithm>
#include <ranges>

// external headers
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/ostream.h>
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
using namespace PMS::JsonUtils;

namespace PMS::Orchestrator {

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
  auto &&tmpresult = m_backDB->Connect();
  tmpresult = m_frontDB->Connect();

  tmpresult = m_backDB->SetupIfNeeded();
  tmpresult = m_frontDB->SetupIfNeeded();

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

ErrorOr<json> Director::ClaimJob(std::string_view pilotUuid) {

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

  DB::Queries::Matches matches{
      {"status",
       std::vector<std::string_view>{magic_enum::enum_name(JobStatus::Pending), magic_enum::enum_name(JobStatus::Error),
                                     magic_enum::enum_name(JobStatus::OutboundTransferError),
                                     magic_enum::enum_name(JobStatus::InboundTransferError)},
       DB::Queries::ComparisonOp::IN},
      {"task", activeTasks, DB::Queries::ComparisonOp::IN},
  };
  if (pilotInfo.tags.empty()) {
    matches.emplace_back("tags", "array", DB::Queries::ComparisonOp::TYPE);
    matches.emplace_back("tags", std::vector<std::string>(), DB::Queries::ComparisonOp::EQ);
  } else {
    matches.emplace_back("tags", pilotInfo.tags, DB::Queries::ComparisonOp::ALL);
  }

  DB::Queries::Updates update_action{
      {"status", magic_enum::enum_name(JobStatus::Claimed)},
      {"pilotUuid", pilotUuid},
      {"retries", 1, DB::Queries::UpdateOp::INC},
  };

  // NOTE(vformato): only keep fields that the pilot will actually need... This alleviates load on the DB
  json projectionOpt = R"({"_id":0, "dataset":0, "jobName":0, "status":0, "tags":0, "user":0})"_json;

  auto query_result = TRY(m_frontDB->RunQuery(DB::Queries::FindOneAndUpdate{
      .collection = "jobs",
      .match = matches,
      .update = update_action,
      .filter = projectionOpt,
  }));

  if (!query_result.empty()) {
    return query_result;
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

      DB::Queries::Matches matches{
          {"status",
           std::vector<std::string_view>{magic_enum::enum_name(JobStatus::Error),
                                         magic_enum::enum_name(JobStatus::InboundTransferError),
                                         magic_enum::enum_name(JobStatus::OutboundTransferError)},
           DB::Queries::ComparisonOp::IN},
          {"retries", m_maxRetries, DB::Queries::ComparisonOp::GTE}};

      DB::Queries::Updates update_action{
          {"status", magic_enum::enum_name(JobStatus::Failed)},
      };

      auto result = m_frontDB->RunQuery(DB::Queries::Update{
          .collection = "jobs",
          .match = matches,
          .update = update_action,
      });

      if (!result) {
        spdlog::error("Failed to update failed jobs");
      }
    }
  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

ErrorOr<Director::NewPilotResult>
Director::RegisterNewPilot(std::string_view pilotUuid, std::string_view user,
                           const std::vector<std::pair<std::string, std::string>> &tasks,
                           const std::vector<std::string> &tags, const json &host_info) {
  NewPilotResult result{OperationResult::Success, {}, {}};

  m_logger->trace("Registering new pilot: {}", pilotUuid);

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

  auto query_result = TRY(m_frontDB->RunQuery(DB::Queries::Insert{
      .collection = "pilots",
      .documents = {query},
  }));

  return result;
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
      if (auto maybe_pilotInfo = GetPilotInfo(uuid); !maybe_pilotInfo)
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

ErrorOr<void> Director::DeleteHeartBeat(std::string_view pilotUuid) {

  if (auto pilotIt = m_activePilots.find(std::string{pilotUuid}); pilotIt != end(m_activePilots))
    m_activePilots.erase(pilotIt);

  DB::Queries::Matches matches{{"uuid", pilotUuid}};

  auto query_result = TRY(m_frontDB->RunQuery(DB::Queries::Delete{
      .collection = "pilots",
      .options = {.limit = 1},
      .match = matches,
  }));

  return outcome::success();
}

ErrorOr<void> Director::AddTaskDependency(const std::string &task, const std::string &dependsOn) {
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

  DB::Queries::Matches matches{{"name", task}};
  DB::Queries::Updates update_action{{"dependencies", dependsOn, DB::Queries::UpdateOp::PUSH}};
  auto result = TRY(m_backDB->RunQuery(DB::Queries::Update{
      .collection = "tasks",
      .options = {.limit = 1},
      .match = matches,
      .update = update_action,
  }));

  return outcome::success();
}

ErrorOr<std::string> Director::CreateTask(const std::string &task) {
  if (m_tasks.find(task) != end(m_tasks)) {
    return Error{std::errc::file_exists, "Task already exists"};
  }

  m_logger->trace("Creating task {}", task);

  // generate a random token
  std::string token = boost::uuids::to_string(boost::uuids::random_generator()());

  auto &newTask = m_tasks[task];
  newTask.name = task;
  newTask.token = token;

  // insert new task in backend DB
  json insertQuery;
  insertQuery["name"] = newTask.name;
  insertQuery["token"] = newTask.token;

  auto result = TRY(m_backDB->RunQuery(DB::Queries::Insert{
      .collection = "tasks",
      .documents = {insertQuery},
  }));

  return token;
}

ErrorOr<void> Director::ClearTask(const std::string &task, bool deleteTask) {

  DB::Queries::Matches filter{{"task", task}};
  auto query_result = TRY(m_backDB->RunQuery(DB::Queries::Delete{
      .collection = "jobs",
      .match = filter,
  }));
  query_result = TRY(m_frontDB->RunQuery(DB::Queries::Delete{
      .collection = "jobs",
      .match = filter,
  }));

  if (deleteTask) {
    DB::Queries::Matches task_filter{{"name", task}};
    query_result = TRY(m_backDB->RunQuery(DB::Queries::Delete{
        .collection = "tasks",
        .match = task_filter,
    }));
    m_tasks.erase(task);
    m_logger->debug("Task {} deleted", task);
  }

  return outcome::success();
}

void Director::JobInsert() {
  static constexpr auto coolDown = std::chrono::milliseconds(10);

  std::vector<json> toBeInserted;
  do {
    while (!m_incomingJobs.empty()) {
      auto job = m_incomingJobs.consume();

      json jobQuery;
      jobQuery["task"] = job["task"];
      jobQuery["hash"] = job["hash"];

      DB::Queries::Matches matches{
          {"task", job["task"]},
          {"hash", job["hash"]},
      };

      // check if this job is already in back-end database
      auto query_result = m_backDB->RunQuery(DB::Queries::Find{
          .collection = "jobs",
          .options = {.limit = 1},
          .match = matches,
      });

      if (!query_result) {
        m_logger->error("Failed to query backend DB for job {}", to_string_view(job["hash"]));
        continue;
      }

      if (query_result && query_result.value())
        continue;

      m_logger->trace("Queueing up job {} for insertion", to_string_view(job["hash"]));

      // job initial status should always be Pending :)
      job["status"] = magic_enum::enum_name(JobStatus::Pending);
      toBeInserted.push_back(job);
    }

    if (!toBeInserted.empty()) {
      m_logger->trace("Inserting {} new jobs into backend DB", toBeInserted.size());

      auto result = m_backDB->RunQuery(DB::Queries::Insert{
          .collection = "jobs",
          .documents = toBeInserted,
      });

      toBeInserted.clear();
    }
  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

void Director::JobTransfer() {
  static constexpr auto coolDown = std::chrono::seconds(10);

  auto bHandle = m_backPoolHandle->DBHandle();
  auto fHandle = m_frontPoolHandle->DBHandle();

  std::vector<json> toBeInserted;
  std::vector<mongocxx::model::write> writeOps;

  do {
    // collect jobs from each active task
    for (const auto &[name, task] : m_tasks) {
      if (!task.readyForScheduling || !task.IsActive()) {
        continue;
      }

      DB::Queries::Matches matches{
          {"task", task.name},
          {"inFrontDB", false, DB::Queries::ComparisonOp::EXISTS},
      };

      auto maybe_query_result = m_backDB->RunQuery(DB::Queries::Find{
          .collection = "jobs",
          .match = matches,
      });

      if (!maybe_query_result) {
        m_logger->error("Failed to query backend DB for jobs");
        continue;
      }

      for (auto &job : maybe_query_result.value()) {
        // filtering out the _id field
        job.erase("_id");
        toBeInserted.push_back(job);
      }
    }

    if (!toBeInserted.empty()) {
      m_logger->debug("Inserting {} new jobs into frontend DB", toBeInserted.size());
      auto insert_result = m_frontDB->RunQuery(DB::Queries::Insert{
          .collection = "jobs",
          .documents = toBeInserted,
      });

      if (!insert_result) {
        m_logger->error("Failed to insert jobs into frontend DB");
        continue;
      }

      std::transform(begin(toBeInserted), end(toBeInserted), std::back_inserter(writeOps), [](const auto &job) {
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

  do {
    m_logger->debug("Updating tasks");

    // get list of all tasks
    auto maybe_tasksResult = m_backDB->RunQuery(DB::Queries::Find{
        .collection = "tasks",
        .filter = "{}"_json,
    });

    if (!maybe_tasksResult) {
      m_logger->error("Failed to query backend DB for tasks");
      continue;
    }

    auto tasksResult = maybe_tasksResult.value();

    for (const auto &tmpdoc : tasksResult) {

      // find task in internal task list
      std::string taskName = to_string(tmpdoc["name"]);

      Task &task = m_tasks[taskName];
      if (task.name.empty()) {
        m_logger->warn("Task {} is in DB but was not found in memory", taskName);

        task.name = taskName;
        task.token = to_string(tmpdoc["token"]);
        if (tmpdoc.contains("dependencies")) {
          std::ranges::transform(tmpdoc["dependencies"], std::back_inserter(task.dependencies),
                                 [](const auto &dep) { return to_string(dep); });
        }
      }

      // skip stale failed tasks, since failed jobs won't be tried anymore
      if (!task.IsActive() && task.IsFailed()) {
        continue;
      }

      auto markTaskAsFailed = [this](const std::string_view taskName) -> ErrorOr<void> {
        DB::Queries::Matches filter{
            {"task", taskName},
        };

        DB::Queries::Updates updateQuery{
            {"status", magic_enum::enum_name(JobStatus::Failed)},
            {"lastUpdate", true, DB::Queries::UpdateOp::CURRENT_DATE},
        };

        TRY(m_frontDB->RunQuery(DB::Queries::Update{
            .collection = "jobs",
            .match = filter,
            .update = updateQuery,
        }));

        return outcome::success();
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
            if (auto maybe_result = markTaskAsFailed(task.name); !maybe_result) {
              m_logger->error("Failed to mark task {} as failed", task.name);
            }
            continue;
          }

          taskIsReady &= requiredTaskIt->second.IsFinished();
        }

        task.readyForScheduling = taskIsReady;
      }

      if (auto update_result = UpdateTaskCounts(task); !update_result) {
        m_logger->error("Failed to update task counts for task {}", task.name);
      }

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

ErrorOr<void> Director::UpdateTaskCounts(Task &task) { // update job counters in task
  auto backHandle = m_backPoolHandle->DBHandle();

  DB::Queries::Matches matches{{"task", task.name}};
  auto query_result = TRY(m_backDB->RunQuery(DB::Queries::Count{
      .collection = "jobs",
      .match = matches,
  }));
  task.totJobs = query_result["count"].get<unsigned int>();

  for (auto status : magic_enum::enum_values<JobStatus>()) {
    matches = {{"task", task.name}, {"status", magic_enum::enum_name(status)}};
    query_result = TRY(m_backDB->RunQuery(DB::Queries::Count{
        .collection = "jobs",
        .match = matches,
    }));
    task.jobs[status] = query_result["count"].get<unsigned int>();
  }

  return outcome::success();
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
      m_logger->debug("Removing dead pilot {}", to_string_view(pilot["uuid"]));

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
        m_logger->debug("Dead pilot {} had a running job ({}), setting to Error...", to_string_view(pilot["uuid"]),
                        to_string_view(job["hash"]));
        UpdateJobStatus(to_string_view(pilot["uuid"]), to_string_view(job["hash"]), to_string_view(job["task"]),
                        JobStatus::Error);
      }

      requests.push_back(mongocxx::model::delete_one(JsonUtils::json2bson(deleteQuery)));
      // RetryIfFailsWith<mongocxx::exception>([&]() { handle["pilots"].delete_one(JsonUtils::json2bson(deleteQuery));
      // });

      if (auto pilotIt = m_activePilots.find(to_string(pilot["uuid"])); pilotIt != end(m_activePilots))
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

ErrorOr<Director::PilotInfo> Director::GetPilotInfo(std::string_view uuid) {
  if (auto uuidString = std::string{uuid}; m_activePilots.find(uuidString) != end(m_activePilots)) {
    return m_activePilots[uuidString];
  } else {
    auto handle = m_frontPoolHandle->DBHandle();
    PilotInfo result;

    auto pilot_info_from_db = TRY(m_frontDB->RunQuery(DB::Queries::Find{
        .collection = "pilots",
        .options{.limit = 1},
        .match = {{"uuid", uuid}},
    }))[0];

    std::ranges::transform(pilot_info_from_db["tasks"], std::back_inserter(result.tasks),
                           [](const auto &task) { return to_string(task); });
    std::ranges::transform(pilot_info_from_db["tags"], std::back_inserter(result.tags),
                           [](const auto &tag) { return to_string(tag); });
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

  auto tasksQueryResult =
      RetryIfFailsWith<mongocxx::exception>([&]() { return handle["jobs"].aggregate(aggregationPipeline); });
  for (auto item : tasksQueryResult) {
    json result = JsonUtils::bson2json(item);
    auto taskName = to_string(result["_id"]);

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

ErrorOr<std::string> Director::QueryBackDB(QueryOperation operation, const json &match, const json &option) const {
  auto backHandle = m_backPoolHandle->DBHandle();
  auto frontHandle = m_frontPoolHandle->DBHandle();

  m_logger->debug("QueryBackDB: {} {} {}", magic_enum::enum_name(operation), match.dump(), option.dump());

  switch (operation) {
  case QueryOperation::UpdateOne: {
    auto matches = TRY(PMS::DB::Queries::ToMatches(match));
    auto update_action = TRY(PMS::DB::Queries::ToUpdates(option));
    update_action.emplace_back("lastUpdate", true, DB::Queries::UpdateOp::CURRENT_DATE);

    auto result = TRY(m_frontDB->RunQuery(DB::Queries::Update{
        .collection = "jobs",
        .options = {.limit = 1},
        .match = matches,
        .update = update_action,
    }));

    return fmt::format("Updated job {}", to_string_view(match["hash"]));
  }
  case QueryOperation::UpdateMany: {
    auto matches = TRY(PMS::DB::Queries::ToMatches(match));
    auto update_action = TRY(PMS::DB::Queries::ToUpdates(option));
    update_action.emplace_back("lastUpdate", true, DB::Queries::UpdateOp::CURRENT_DATE);

    auto result = TRY(m_frontDB->RunQuery(DB::Queries::Update{
        .collection = "jobs",
        .match = matches,
        .update = update_action,
    }));

    return fmt::format("Matched {} jobs. Updated {} jobs", to_string(result["matched_count"]),
                       to_string(result["modified_count"]));
  }
  case QueryOperation::DeleteOne: {
    auto matches = TRY(PMS::DB::Queries::ToMatches(match));

    auto result = TRY(m_frontDB->RunQuery(DB::Queries::Delete{
        .collection = "jobs",
        .options = {.limit = 1},
        .match = matches,
    }));

    result = TRY(m_backDB->RunQuery(DB::Queries::Delete{
        .collection = "jobs",
        .options = {.limit = 1},
        .match = matches,
    }));

    return fmt::format("Deleted job {}", to_string_view(match["hash"]));
  }
  case QueryOperation::DeleteMany: {
    auto matches = TRY(PMS::DB::Queries::ToMatches(match));

    auto result = TRY(m_frontDB->RunQuery(DB::Queries::Delete{
        .collection = "jobs",
        .match = matches,
    }));

    result = TRY(m_backDB->RunQuery(DB::Queries::Delete{
        .collection = "jobs",
        .match = matches,
    }));

    return fmt::format("Deleted {} jobs", to_string_view(result["deleted_count"]));
  }
  case QueryOperation::Find: {
    json projectionOpt;
    if (option.empty()) {
      projectionOpt = R"({"_id":0})"_json;
    } else {
      projectionOpt = option;
      projectionOpt["_id"] = 0;
    }

    auto matches = TRY(PMS::DB::Queries::ToMatches(match));

    json resp;
    resp["result"] = TRY(m_backDB->RunQuery(DB::Queries::Find{
        .collection = "jobs",
        .match = matches,
        .filter = projectionOpt,
    }));

    return resp.dump();
  }
  }

  return Error(std::errc::not_supported, "Operation not supported");
}

ErrorOr<std::string> Director::QueryFrontDB(DBCollection collection, const json &match, const json &filter) const {

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

  DB::Queries::Matches matches = TRY(PMS::DB::Queries::ToMatches(match));
  auto query_result = TRY(m_frontDB->RunQuery(DB::Queries::Find{
      .collection = collection_name.data(),
      .match = matches,
      .filter = projectionOpt,
  }));

  json resp;
  resp["result"] = query_result;

  return resp.dump();
}

ErrorOr<void> Director::ResetFailedJobs(std::string_view taskname) {

  DB::Queries::Matches matches = {
      {"task", taskname},
      {"status", magic_enum::enum_name(JobStatus::Failed)},
  };

  DB::Queries::Updates update_action{
      {"status", magic_enum::enum_name(JobStatus::Pending)},
      {"retries", 0},
  };

  auto result = TRY(m_frontDB->RunQuery(DB::Queries::Update{
      .collection = "jobs",
      .match = matches,
      .update = update_action,
  }));
  m_logger->debug("ResetFailedJobs: Found {} documents in front DB to update",
                  result["matched_count"].get<unsigned int>());

  result = TRY(m_backDB->RunQuery(DB::Queries::Update{
      .collection = "jobs",
      .match = matches,
      .update = update_action,
  }));
  m_logger->debug("ResetFailedJobs: Found {} documents in back DB to update",
                  result["matched_count"].get<unsigned int>());

  m_logger->debug("Reset all failed jobs in taskname {}", taskname);

  std::string s_taskname{taskname};
  TRY(UpdateTaskCounts(m_tasks.at(s_taskname)));

  return outcome::success();
}

} // namespace PMS::Orchestrator
