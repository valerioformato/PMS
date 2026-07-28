// c++ headers
#include <algorithm>
#include <ranges>

// external headers
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <mongocxx/bulk_write.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/pipeline.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/fmt/bundled/ostream.h>
#include <spdlog/fmt/bundled/ranges.h>

// our headers
#include "common/Async.h"
#include "common/Job.h"
#include "common/JsonUtils.h"
#include "orchestrator/Director.h"

using json = nlohmann::json;
using namespace PMS::JsonUtils;

namespace PMS::Orchestrator {

void Director::Start() {
  auto tmpresult = m_backDB->Connect();
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

Async<ErrorOr<json>> Director::PilotClaimJob(std::string_view pilotUuid) {
  static constexpr std::string_view logPrefix = "PilotClaimJob: ";
  auto scheduler = co_await stdexec::read_env(stdexec::get_scheduler);

  auto maybe_pilot_info = co_await GetPilotInfo(pilotUuid);
  if (!maybe_pilot_info) {
    co_return R"({"error": "unknown pilot"})"_json;
  }

  const auto &pilot_info = maybe_pilot_info.value();

  bool done = true;
  for (const auto &taskName : pilot_info.tasks) {
    done &= !(m_tasks[taskName].IsActive());
  }

  // NOTE(vformato): if all tasks are inactive, we can't assign any job
  if (done)
    co_return R"({"finished": true})"_json;

  done = true;
  for (const auto &taskName : pilot_info.tasks) {
    done &= (m_tasks[taskName].IsExhausted());
  }

  // NOTE(vformato): if at least one task is exhausted but not finished, we can't assign any job, but jobs are allowed
  // to go to error and be retried
  if (done)
    co_return R"({"sleep": true})"_json;

  // NOTE(vformato): check which tasks are currently active for this pilot
  std::vector<std::string_view> active_tasks;
  std::ranges::copy_if(pilot_info.tasks, std::back_inserter(active_tasks),
                       [this](const auto &taskName) { return m_tasks[taskName].IsActive(); });

  DB::Queries::Matches matches{
      {"status",
       std::vector<std::string_view>{magic_enum::enum_name(JobStatus::Pending), magic_enum::enum_name(JobStatus::Error),
                                     magic_enum::enum_name(JobStatus::OutboundTransferError),
                                     magic_enum::enum_name(JobStatus::InboundTransferError)},
       DB::Queries::ComparisonOp::IN},
      {"task", active_tasks, DB::Queries::ComparisonOp::IN},
  };
  if (pilot_info.tags.empty()) {
    matches.emplace_back("tags", "array", DB::Queries::ComparisonOp::TYPE);
    matches.emplace_back("tags", std::vector<std::string>(), DB::Queries::ComparisonOp::EQ);
  } else {
    matches.emplace_back("tags", pilot_info.tags, DB::Queries::ComparisonOp::ALL);
  }

  // NOTE(vformato): only keep fields that the pilot will actually need... This alleviates load on the DB
  json projection_opt = R"({"_id":0, "dataset":0, "jobName":0, "status":0, "tags":0, "user":0})"_json;
  DB::Queries::Updates update_action{
      {"status", magic_enum::enum_name(JobStatus::Claimed)},
      {"pilotUuid", pilotUuid},
      {"retries", 1, DB::Queries::UpdateOp::INC},
      {"lastUpdate", Utils::CurrentTimeToMillisSinceEpoch()},
  };

  auto maybe_query_result = co_await (m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                                          DB::Queries::FindOneAndUpdate{
                                                              .collection = "jobs",
                                                              .match = matches,
                                                              .update = update_action,
                                                              .filter = projection_opt,
                                                          }) |
                                      stdexec::continues_on(scheduler));

  if (!maybe_query_result) {
    m_logger->error("{} Failed to query frontend DB for jobs: {}", logPrefix, maybe_query_result.error().Message());
    co_return R"({"sleep": true})"_json;
  }

  if (maybe_query_result.value().empty()) {
    m_logger->trace("{} no matching job found for pilot {}", logPrefix, pilot_info.uuid);
    co_return R"({"sleep": true})"_json;
  }

  co_return maybe_query_result.value();
}

Async<ErrorOr<void>> Director::UpdateJobStatus(std::string_view pilotUuid, std::string_view hash, std::string_view task,
                                               JobStatus status) {
  auto maybe_pilotInfo = co_await GetPilotInfo(pilotUuid);
  if (!maybe_pilotInfo.has_value()) {
    co_return make_error(std::errc::invalid_argument, fmt::format("Unknown pilot {}", pilotUuid));
  }

  const auto &pilotInfo = maybe_pilotInfo.value();

  auto pilotTasks = pilotInfo.tasks;
  if (std::find(begin(pilotTasks), end(pilotTasks), task) == end(pilotTasks)) {
    co_return make_error(std::errc::invalid_argument,
                         fmt::format("Pilot {} is not allowed to work on task {}", pilotUuid, task));
  }

  DB::Queries::Matches matches{
      {"task", task},
      {"hash", hash},
  };

  auto update_time = Utils::CurrentTimeToMillisSinceEpoch();

  DB::Queries::Updates update_action{
      {"status", magic_enum::enum_name(status)},
  };

  switch (status) {
  case JobStatus::Running:
    update_action.emplace_back("startTime", update_time);
    break;
  case JobStatus::Error:
  case JobStatus::Done:
    update_action.emplace_back("endTime", update_time);
    break;
  default:
    break;
  }

  [[maybe_unused]] std::lock_guard lock{m_jobUpdateRequests_mx};
  m_jobUpdateRequests.push_back(DB::Queries::Update{
      .collection = "jobs",
      .options = {.limit = 1},
      .match = matches,
      .update = update_action,
  });

  co_return {};
}

void Director::WriteJobUpdates() {
  static constexpr auto coolDown = std::chrono::seconds(1);

  do {
    if (!m_jobUpdateRequests.empty()) {
      std::vector<DB::Queries::Query> job_update_requests{};

      {
        std::lock_guard lock{m_jobUpdateRequests_mx};
        std::swap(m_jobUpdateRequests, job_update_requests);
      }

      std::ranges::for_each(job_update_requests, [this](DB::Queries::Query &query) {
        std::get<DB::Queries::Update>(query).update.emplace_back("lastUpdate", Utils::CurrentTimeToMillisSinceEpoch());
      });

      auto write_result = m_frontDB->BulkWrite("jobs", job_update_requests);
      if (!write_result) {
        m_logger->error("Failed to write job updates: {}", write_result.error().Message());
        m_logger->error("Will retry...");

        std::lock_guard lock{m_jobUpdateRequests_mx};
        m_jobUpdateRequests.reserve(m_jobUpdateRequests.size() + job_update_requests.size());
        m_jobUpdateRequests.insert(end(m_jobUpdateRequests), begin(job_update_requests), end(job_update_requests));
        continue;
      }

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

Async<ErrorOr<Director::NewPilotResult>>
Director::RegisterNewPilot(std::string_view pilotUuid, std::string_view user,
                           const std::vector<std::pair<std::string, std::string>> &tasks,
                           const std::vector<std::string> &tags, const json &host_info) {
  auto scheduler = co_await stdexec::read_env(stdexec::get_scheduler);

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
  query["lastHeartBeat"] = Utils::CurrentTimeToMillisSinceEpoch();

  m_activePilots[to_string(pilotUuid)] = PilotInfo{to_string(pilotUuid), result.validTasks, tags};

  auto insert_r = co_await (m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                                DB::Queries::Insert{
                                                    .collection = "pilots",
                                                    .documents = {query},
                                                }) |
                            stdexec::continues_on(scheduler));
  CO_TRY(insert_r);

  co_return result;
}

ErrorOr<void> Director::UpdateHeartBeat(std::string_view pilotUuid) {
  m_heartbeatUpdates.emplace(std::string{pilotUuid}, std::chrono::system_clock::now());
  return {};
}

void Director::WriteHeartBeatUpdates() {
  static constexpr auto coolDown = std::chrono::minutes(1);

  do {
    std::unordered_map<std::string, time_point> unique_hbs;
    auto hbs = m_heartbeatUpdates.consume_all();
    // NOTE: de-duplicate heartbeats. If all is correct, requests are ordered in time, so only most recent one survives
    std::for_each(begin(hbs), end(hbs), [&unique_hbs](const PilotHeartBeat &hb) { unique_hbs[hb.uuid] = hb.time; });

    std::vector<DB::Queries::Query> requests;
    for (const auto &[uuid, time] : unique_hbs) {
      // NOTE: if this pilot is not in the active cache *and* not in the front DB collection then it's either an unknown
      // pilot or it sent an update before dying and being removed from both cache and DB
      bool pilotKnown = m_activePilots.count(uuid) > 0;
      if (!pilotKnown) {
        auto db_result = m_frontDB->RunQuery(DB::Queries::Find{
            .collection = "pilots",
            .options{.limit = 1},
            .match = {{"uuid", uuid}},
        });
        pilotKnown = db_result.has_value() && !db_result->empty();
      }
      if (!pilotKnown)
        continue;

      auto millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(time.time_since_epoch());

      DB::Queries::Matches matches{{"uuid", uuid}};
      DB::Queries::Updates update_action = {{"lastHeartBeat", millis_since_epoch.count()}};

      requests.push_back(DB::Queries::Update{
          .collection = "pilots",
          .options = {.limit = 1},
          .match = matches,
          .update = update_action,
      });
    }

    if (!requests.empty()) {
      m_logger->debug("Updating {} heartbeats", requests.size());
      auto write_result = m_frontDB->BulkWrite("pilots", requests);
      if (!write_result) {
        m_logger->error("Failed to write heartbeat updates: {}", write_result.error().Message());
      }
    }
  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

Async<ErrorOr<void>> Director::DeleteHeartBeat(std::string_view pilotUuid) {
  auto scheduler = co_await stdexec::read_env(stdexec::get_scheduler);

  if (auto pilotIt = m_activePilots.find(std::string{pilotUuid}); pilotIt != end(m_activePilots))
    m_activePilots.erase(pilotIt);

  DB::Queries::Matches matches{{"uuid", pilotUuid}};

  auto qr = co_await (m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                          DB::Queries::Delete{
                                              .collection = "pilots",
                                              .options = {.limit = 1},
                                              .match = matches,
                                          }) |
                      stdexec::continues_on(scheduler));

  auto query_result = CO_TRY(qr);

  co_return {};
}

Async<ErrorOr<void>> Director::AddTaskDependency(const std::string &task, const std::string &dependsOn) {
  auto scheduler = co_await stdexec::read_env(stdexec::get_scheduler);

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
  auto result = co_await (m_backDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                             DB::Queries::Update{
                                                 .collection = "tasks",
                                                 .options = {.limit = 1},
                                                 .match = matches,
                                                 .update = update_action,
                                             }) |
                          stdexec::continues_on(scheduler));

  CO_TRY(result);

  co_return {};
}

Async<ErrorOr<std::string>> Director::CreateTask(const std::string &task) {
  auto scheduler = co_await stdexec::read_env(stdexec::get_scheduler);

  if (m_tasks.find(task) != end(m_tasks)) {
    co_return make_error(std::errc::file_exists, "Task already exists");
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

  auto result = co_await (m_backDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                             DB::Queries::Insert{
                                                 .collection = "tasks",
                                                 .documents = {insertQuery},
                                             }) |
                          stdexec::continues_on(scheduler));

  CO_TRY(result);

  co_return token;
}

Async<ErrorOr<void>> Director::ClearTask(const std::string &task, bool deleteTask) {
  auto scheduler = co_await stdexec::read_env(stdexec::get_scheduler);

  DB::Queries::Matches filter{{"task", task}};

  auto [r1, r2] =
      co_await (stdexec::when_all(m_backDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                                     DB::Queries::Delete{.collection = "jobs", .match = filter}),
                                  m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                                      DB::Queries::Delete{.collection = "jobs", .match = filter})) |
                stdexec::continues_on(scheduler));
  CO_TRY(r1);
  CO_TRY(r2);

  if (deleteTask) {
    DB::Queries::Matches task_filter{{"name", task}};
    auto r3 = co_await (m_backDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                           DB::Queries::Delete{
                                               .collection = "tasks",
                                               .match = task_filter,
                                           }) |
                        stdexec::continues_on(scheduler));
    CO_TRY(r3);
    m_tasks.erase(task);
    m_logger->debug("Task {} deleted", task);
  }

  co_return {};
}

void Director::JobInsert() {
  static constexpr auto coolDown = std::chrono::milliseconds(10);

  std::vector<json> toBeInserted;
  do {
    while (!m_incomingJobs.empty()) {
      auto job = m_incomingJobs.pop();

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

      if (query_result && !query_result.value().empty())
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
  static constexpr auto coolDown = std::chrono::seconds(1);

  m_logger->info("Job transfer thread started with {} jobs per query", m_maxJobTransferQuerySize);

  do {
    // collect jobs from each active task
    for (const auto &[name, task] : m_tasks) {
      std::vector<json> toBeInserted;
      std::vector<DB::Queries::Query> writeOps;

      if (!task.readyForScheduling || !task.IsActive()) {
        continue;
      }

      DB::Queries::Matches matches{
          {"task", task.name},
          {"inFrontDB", false, DB::Queries::ComparisonOp::EXISTS},
      };

      auto maybe_query_result = m_backDB->RunQuery(DB::Queries::Find{
          .collection = "jobs",
          .options = {.limit = m_maxJobTransferQuerySize},
          .match = matches,
      });

      if (!maybe_query_result) {
        m_logger->error("Failed to query backend DB for jobs: {}", maybe_query_result.error().Message());
        continue;
      }

      std::ranges::transform(maybe_query_result.value(), std::back_inserter(toBeInserted), [](auto &job) {
        job.erase("_id");
        return job;
      });

      if (!toBeInserted.empty()) {
        m_logger->debug("Inserting {} new jobs into frontend DB", toBeInserted.size());
        auto insert_result = m_frontDB->RunQuery(DB::Queries::Insert{
            .collection = "jobs",
            .documents = toBeInserted,
        });

        if (!insert_result) {
          m_logger->error("Failed to insert jobs into frontend DB: {}", insert_result.error().Message());
          continue;
        }

        std::ranges::transform(toBeInserted, std::back_inserter(writeOps), [](const auto &job) {
          return DB::Queries::Update{
              .collection = "jobs",
              .options = {.limit = 1},
              .match = {{"hash", job["hash"]}},
              .update = {{"inFrontDB", true}},
          };
        });

        auto write_result = m_backDB->BulkWrite("jobs", writeOps);
        if (!write_result) {
          m_logger->error("Failed to update jobs in backend DB: {}", write_result.error().Message());
        }
      }
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
            {"lastUpdate", Utils::CurrentTimeToMillisSinceEpoch()},
        };

        TRY(m_frontDB->RunQuery(DB::Queries::Update{
            .collection = "jobs",
            .match = filter,
            .update = updateQuery,
        }));

        return {};
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
  DB::Queries::Matches matches{{"task", task.name}};
  for (auto status : magic_enum::enum_values<JobStatus>()) {
    matches = {{"task", task.name}, {"status", magic_enum::enum_name(status)}};
    auto query_result = TRY(m_backDB->RunQuery(DB::Queries::Count{
        .collection = "jobs",
        .match = matches,
    }));
    task.jobs[status] = query_result["count"].get<unsigned int>();
  }

  task.totJobs = std::accumulate(task.jobs.begin(), task.jobs.end(), 0u);

  return {};
}

void Director::UpdateDeadPilots() {
  static constexpr auto coolDown = std::chrono::minutes(10);

  static constexpr std::chrono::system_clock::duration gracePeriod = std::chrono::hours{1};

  do {
    auto last_valid_hearbeat_in_millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(
        (std::chrono::system_clock::now() - gracePeriod).time_since_epoch());
    DB::Queries::Matches matches{
        {"lastHeartBeat", last_valid_hearbeat_in_millis_since_epoch.count(), DB::Queries::ComparisonOp::LT},
    };

    auto pilot_query_result = m_frontDB->RunQuery(DB::Queries::Find{
        .collection = "pilots",
        .match = matches,
    });

    if (!pilot_query_result) {
      m_logger->error("Failed to query frontend DB for dead pilots");
      continue;
    }

    auto queryResult = pilot_query_result.value();

    std::vector<DB::Queries::Query> requests;

    for (const auto &pilot : queryResult) {
      m_logger->debug("Removing dead pilot {}", to_string_view(pilot["uuid"]));

      json deleteQuery;
      deleteQuery["uuid"] = pilot["uuid"];

      DB::Queries::Matches delete_query_match{{"uuid", pilot["uuid"]}};

      DB::Queries::Matches job_query_match{
          {"pilotUuid", pilot["uuid"]},
          {"status",
           std::vector<std::string_view>{magic_enum::enum_name(JobStatus::Running),
                                         magic_enum::enum_name(JobStatus::InboundTransfer),
                                         magic_enum::enum_name(JobStatus::OutboundTransfer)},
           DB::Queries::ComparisonOp::IN},
      };

      json projectionOpt = R"({"_id":1, "hash":1, "task":1})"_json;

      auto job_query_result = m_frontDB->RunQuery(DB::Queries::Find{
          .collection = "jobs",
          .options = {.limit = 1},
          .match = job_query_match,
          .filter = projectionOpt,
      });

      if (!job_query_result) {
        m_logger->error("Failed to query frontend DB for running jobs of dead pilot {}", to_string_view(pilot["uuid"]));
        continue;
      }

      if (!job_query_result.value().empty()) {
        json job = std::move(job_query_result.value().front());
        m_logger->debug("Dead pilot {} had a running job ({}), setting to Error...", to_string_view(pilot["uuid"]),
                        to_string_view(job["hash"]));

        {
          DB::Queries::Matches job_match{{"task", job["task"]}, {"hash", job["hash"]}};
          DB::Queries::Updates job_update{
              {"status", magic_enum::enum_name(JobStatus::Error)},
              {"endTime", Utils::CurrentTimeToMillisSinceEpoch()},
          };
          std::lock_guard lock{m_jobUpdateRequests_mx};
          m_jobUpdateRequests.push_back(DB::Queries::Update{
              .collection = "jobs",
              .options = {.limit = 1},
              .match = job_match,
              .update = job_update,
          });
        }
      }

      requests.push_back(DB::Queries::Delete{
          .collection = "pilots",
          .match = delete_query_match,
      });

      if (auto pilotIt = m_activePilots.find(to_string(pilot["uuid"])); pilotIt != end(m_activePilots))
        m_activePilots.erase(pilotIt);
    }

    if (!requests.empty()) {
      m_logger->debug("Deleting {} pilots from front DB", requests.size());
      auto delete_query_result = m_frontDB->BulkWrite("pilots", requests);
      if (!delete_query_result) {
        m_logger->error("Failed to delete dead pilots from the front DB");
      }
    }
  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

void Director::DBSync() {
  static constexpr auto coolDown = std::chrono::seconds(60);

  static time_point lastCheck = std::chrono::system_clock::now();

  do {
    // NOTE: as a safety measure, we add a 10 seconds buffer to the last check time. Some jobs might get copied twice
    // but it's better than missing some
    auto last_check_in_millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(
        (lastCheck - std::chrono::seconds(10)).time_since_epoch());
    DB::Queries::Matches matches{
        {"lastUpdate", last_check_in_millis_since_epoch.count(), DB::Queries::ComparisonOp::GT},
    };

    m_logger->debug("Syncing DBs... (lastUpdate > {})", last_check_in_millis_since_epoch.count());

    lastCheck = std::chrono::system_clock::now();
    auto query_result = m_frontDB->RunQuery(DB::Queries::Find{
        .collection = "jobs",
        .match = matches,
    });

    if (!query_result) {
      m_logger->error("Failed to query frontend DB for updated jobs: {}", query_result.error().Message());
      continue;
    }

    m_logger->debug("...query done.");

    // auto chunks = query_result | std::views::chunk(m_maxJobTransferQuerySize);
    std::vector<DB::Queries::Query> writeOps;
    std::ranges::transform(query_result.value(), std::back_inserter(writeOps), [](const auto &job) {
      DB::Queries::Matches job_query_match{{"hash", job["hash"]}};

      DB::Queries::Updates job_update_action = {{"status", job["status"]}};

      if (auto status = magic_enum::enum_cast<JobStatus>(to_string_view(job["status"])); status.has_value()) {
        if (status.value() == JobStatus::Running) {
          job_update_action.emplace_back("startTime", job["startTime"]);
          job_update_action.emplace_back("pilotUuid", job["pilotUuid"]);
        }
        if ((status.value() == JobStatus::Done || status.value() == JobStatus::Error) && job.contains("endTime")) {
          job_update_action.emplace_back("endTime", job["endTime"]);
        }
      }

      return DB::Queries::Update{
          .collection = "jobs",
          .options = {.limit = 1},
          .match = job_query_match,
          .update = job_update_action,
      };
    });

    if (!writeOps.empty()) {
      auto write_result = m_backDB->BulkWrite("jobs", writeOps);
      if (!write_result) {
        m_logger->error("Failed to write job updates to backend DB");
      }
    }

    m_logger->debug("DBs synced: {} jobs updated in backend DB", writeOps.size());
  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

Director::OperationResult Director::ValidateTaskToken(std::string_view task, std::string_view token) const {
  if (auto taskIt = m_tasks.find(std::string{task}); taskIt != end(m_tasks)) {
    return taskIt->second.token == token ? OperationResult::Success : OperationResult::ProcessError;
  }

  return OperationResult::DatabaseError;
}

Async<ErrorOr<Director::PilotInfo>> Director::GetPilotInfo(std::string_view uuid) {
  auto uuidString = std::string{uuid};
  if (m_activePilots.find(uuidString) != end(m_activePilots)) {
    co_return m_activePilots[uuidString];
  }

  auto scheduler = co_await stdexec::read_env(stdexec::get_scheduler);

  PilotInfo result;
  result.uuid = uuid;

  auto r = co_await (m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                         DB::Queries::Find{
                                             .collection = "pilots",
                                             .options{.limit = 1},
                                             .match = {{"uuid", uuid}},
                                         }) |
                     stdexec::continues_on(scheduler));
  auto pilot_info_json = CO_TRY(r);
  auto pilot_info_from_db = pilot_info_json[0];

  std::ranges::transform(pilot_info_from_db["tasks"], std::back_inserter(result.tasks),
                         [](const auto &task) { return to_string(task); });
  std::ranges::transform(pilot_info_from_db["tags"], std::back_inserter(result.tags),
                         [](const auto &tag) { return to_string(tag); });
  m_activePilots[uuidString] = result;
  co_return result;
}

Async<ErrorOr<std::string>> Director::Summary(const std::string &user) {
  json summary = json::array({});

  DB::Queries::Matches matches{{"user", user}};
  auto scheduler = co_await stdexec::read_env(stdexec::get_scheduler);
  auto r = co_await (m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                         DB::Queries::Distinct{
                                             .collection = "jobs",
                                             .field = "task",
                                             .match = matches,
                                         }) |
                     stdexec::continues_on(scheduler));
  auto tasksQueryResult = CO_TRY(r);

  for (auto item : tasksQueryResult) {
    auto taskName = to_string(item);

    if (m_tasks.find(taskName) == end(m_tasks)) {
      m_logger->error("Task {} not found in internal task list", taskName);
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

  co_return summary.dump();
}

Async<ErrorOr<std::string>> Director::QueryBackDB(QueryOperation operation, const json &match, const json &option) {
  m_logger->debug("QueryBackDB: {} {} {}", magic_enum::enum_name(operation), match.dump(), option.dump());

  auto scheduler = co_await stdexec::read_env(stdexec::get_scheduler);

  switch (operation) {
  case QueryOperation::UpdateOne: {
    auto matches = CO_TRY(PMS::DB::Queries::ToMatches(match));
    auto update_action = CO_TRY(PMS::DB::Queries::ToUpdates(option));
    update_action.emplace_back("lastUpdate", Utils::CurrentTimeToMillisSinceEpoch());

    auto r = co_await (m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                           DB::Queries::Update{
                                               .collection = "jobs",
                                               .options = {.limit = 1},
                                               .match = matches,
                                               .update = update_action,
                                           }) |
                       stdexec::continues_on(scheduler));
    CO_TRY(r);
    co_return fmt::format("Updated job {}", to_string_view(match["hash"]));
  }
  case QueryOperation::UpdateMany: {
    auto matches = CO_TRY(PMS::DB::Queries::ToMatches(match));
    auto update_action = CO_TRY(PMS::DB::Queries::ToUpdates(option));
    update_action.emplace_back("lastUpdate", Utils::CurrentTimeToMillisSinceEpoch());

    auto r = co_await (m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                           DB::Queries::Update{
                                               .collection = "jobs",
                                               .match = matches,
                                               .update = update_action,
                                           }) |
                       stdexec::continues_on(scheduler));
    auto result = CO_TRY(r);
    co_return fmt::format("Matched {} jobs. Updated {} jobs", result["matched_count"].get<size_t>(),
                          result["modified_count"].get<size_t>());
  }
  case QueryOperation::DeleteOne: {
    auto matches = CO_TRY(PMS::DB::Queries::ToMatches(match));
    auto [r1, r2] = co_await (
        stdexec::when_all(
            m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                DB::Queries::Delete{.collection = "jobs", .options = {.limit = 1}, .match = matches}),
            m_backDB->RunQuery(m_io_thread_pool.get_scheduler(),
                               DB::Queries::Delete{.collection = "jobs", .options = {.limit = 1}, .match = matches})) |
        stdexec::continues_on(scheduler));
    CO_TRY(r1);
    CO_TRY(r2);
    co_return fmt::format("Deleted job {}", to_string_view(match["hash"]));
  }
  case QueryOperation::DeleteMany: {
    auto matches = CO_TRY(PMS::DB::Queries::ToMatches(match));
    auto [r1, r2] =
        co_await (stdexec::when_all(m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                                        DB::Queries::Delete{.collection = "jobs", .match = matches}),
                                    m_backDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                                       DB::Queries::Delete{.collection = "jobs", .match = matches})) |
                  stdexec::continues_on(scheduler));
    CO_TRY(r1);
    auto back_result = CO_TRY(r2);
    co_return fmt::format("Deleted {} jobs", back_result["deleted_count"].get<size_t>());
  }
  case QueryOperation::Find: {
    json projectionOpt;
    if (option.empty()) {
      projectionOpt = R"({"_id":0})"_json;
    } else {
      projectionOpt = option;
      projectionOpt["_id"] = 0;
    }

    auto matches = CO_TRY(PMS::DB::Queries::ToMatches(match));
    auto r = co_await (m_backDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                          DB::Queries::Find{
                                              .collection = "jobs",
                                              .match = matches,
                                              .filter = projectionOpt,
                                          }) |
                       stdexec::continues_on(scheduler));
    json resp;
    resp["result"] = CO_TRY(r);
    co_return resp.dump();
  }
  }

  co_return make_error(std::errc::not_supported, "Operation not supported");
}

Async<ErrorOr<std::string>> Director::QueryFrontDB(DBCollection collection, const json &match, const json &filter) {
  m_logger->debug("QueryFrontDB: {} {} {}", magic_enum::enum_name(collection), match.dump(), filter.dump());

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

  DB::Queries::Matches matches = CO_TRY(PMS::DB::Queries::ToMatches(match));
  auto scheduler = co_await stdexec::read_env(stdexec::get_scheduler);
  auto r = co_await (m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(),
                                         DB::Queries::Find{
                                             .collection = collection_name.data(),
                                             .match = matches,
                                             .filter = projectionOpt,
                                         }) |
                     stdexec::continues_on(scheduler));
  auto query_result = CO_TRY(r);

  json resp;
  resp["result"] = query_result;

  co_return resp.dump();
}

Async<ErrorOr<void>> Director::ResetFailedJobs(std::string_view taskname) {

  DB::Queries::Matches matches = {
      {"task", taskname},
      {"status", magic_enum::enum_name(JobStatus::Failed)},
  };

  DB::Queries::Updates update_action{
      {"status", magic_enum::enum_name(JobStatus::Pending)},
      {"retries", 0},
  };

  auto scheduler = co_await stdexec::read_env(stdexec::get_scheduler);

  auto [r1, r2] = co_await (
      stdexec::when_all(
          m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(),
                              DB::Queries::Update{.collection = "jobs", .match = matches, .update = update_action}),
          m_backDB->RunQuery(m_io_thread_pool.get_scheduler(),
                             DB::Queries::Update{.collection = "jobs", .match = matches, .update = update_action})) |
      stdexec::continues_on(scheduler));

  auto front_result = CO_TRY(r1);
  m_logger->debug("ResetFailedJobs: Found {} documents in front DB to update",
                  front_result["matched_count"].get<unsigned int>());

  auto back_result = CO_TRY(r2);
  m_logger->debug("ResetFailedJobs: Found {} documents in back DB to update",
                  back_result["matched_count"].get<unsigned int>());

  m_logger->debug("Reset all failed jobs in taskname {}", taskname);

  std::string s_taskname{taskname};
  auto tc_r = UpdateTaskCounts(m_tasks.at(s_taskname));
  CO_TRY(tc_r);

  co_return {};
}

} // namespace PMS::Orchestrator
