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
  m_threads.emplace_back(&Director::RunClaimQueries, this);
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

ErrorOr<json> Director::PilotClaimJob(std::string_view pilotUuid) {

  auto maybe_pilot_info = GetPilotInfo(pilotUuid);
  if (!maybe_pilot_info) {
    return R"({"error": "unknown pilot"})"_json;
  }

  const auto &pilot_info = maybe_pilot_info.value();

  bool done = true;
  for (const auto &taskName : pilot_info.tasks) {
    done &= !(m_tasks[taskName].IsActive());
  }

  // NOTE(vformato): if all tasks are inactive, we can't assign any job
  if (done)
    return R"({"finished": true})"_json;

  done = true;
  for (const auto &taskName : pilot_info.tasks) {
    done &= (m_tasks[taskName].IsExhausted());
  }

  // NOTE(vformato): if at least one task is exhausted but not finished, we can't assign any job, but jobs are allowed
  // to go to error and be retried
  if (done)
    return R"({"sleep": true})"_json;

  m_claimRequests.push(pilot_info);

  // NOTE(vformato): wait until we find a job for this pilot
  std::string pilotUuidStr{pilotUuid};
  while (m_claimedJobs.find(pilotUuidStr) == end(m_claimedJobs)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // NOTE(vformato): wait until job is marked as claimed
  while (!m_claimedJobs[pilotUuidStr].claimed) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // NOTE(vformato): mark the job as sent for cleanup
  auto job = m_claimedJobs[pilotUuidStr].job;
  m_claimedJobs[pilotUuidStr].sent = true;

  return job;
}

void Director::RunClaimQueries() {
  static constexpr std::string_view logPrefix = "RunClaimQueries: ";
  static constexpr auto coolDown = std::chrono::seconds(1);

  std::unordered_map<std::string, std::deque<json>> queried_job_pool;

  do {

    // NOTE(vformato): remove all jobs that have been sent to pilots
    auto erased = std::erase_if(m_claimedJobs, [](const auto &p_it) { return p_it.second.sent; });
    if (erased > 0 || !m_claimedJobs.empty()) {
      m_logger->trace("{} Erased {} jobs already sent. {} remaining", logPrefix, erased, m_claimedJobs.size());
    }

    auto pilot_infos = m_claimRequests.consume_all();
    size_t query_limit = pilot_infos.size();

    auto get_active_task =
        [&queried_job_pool](std::vector<std::string_view> activeTasks) -> std::optional<std::string> {
      std::string selected_task;
      return std::ranges::any_of(queried_job_pool,
                                 [&](auto p_it) {
                                   const auto &[task, jobs] = p_it;
                                   if (std::ranges::find(activeTasks, task) != end(activeTasks) && !jobs.empty()) {
                                     selected_task = task;
                                     return true;
                                   } else {
                                     return false;
                                   }
                                 })
                 ? std::optional{selected_task}
                 : std::nullopt;
    };

    for (const auto &pilot_info : pilot_infos) {
      // NOTE(vformato): check which tasks are currently active for this pilot
      std::vector<std::string_view> activeTasks;
      std::copy_if(begin(pilot_info.tasks), end(pilot_info.tasks), std::back_inserter(activeTasks),
                   [this](const auto &taskName) { return m_tasks[taskName].IsActive(); });

      // NOTE(vformato): check if we already have jobs queried for these tasks
      if (auto maybe_active_task = get_active_task(activeTasks); maybe_active_task) {
        const auto &selected_task = maybe_active_task.value();

        // take job from the queue for this pilot
        const auto job = queried_job_pool[selected_task].front();
        queried_job_pool[selected_task].pop_front();

        m_logger->trace("{} Pilot {} has jobs available in active task {}: selected job {}", logPrefix, pilot_info.uuid,
                        selected_task, to_string_view(job["hash"]));

        m_claimedJobs[pilot_info.uuid] = ClaimedJob{.job = job};

        --query_limit;
        continue;
      }

      // NOTE(vformato): we have no jobs available for this pilot. We have to query the DB
      DB::Queries::Matches matches{
          {"status",
           std::vector<std::string_view>{magic_enum::enum_name(JobStatus::Pending),
                                         magic_enum::enum_name(JobStatus::Error),
                                         magic_enum::enum_name(JobStatus::OutboundTransferError),
                                         magic_enum::enum_name(JobStatus::InboundTransferError)},
           DB::Queries::ComparisonOp::IN},
          {"task", activeTasks, DB::Queries::ComparisonOp::IN},
      };
      if (pilot_info.tags.empty()) {
        matches.emplace_back("tags", "array", DB::Queries::ComparisonOp::TYPE);
        matches.emplace_back("tags", std::vector<std::string>(), DB::Queries::ComparisonOp::EQ);
      } else {
        matches.emplace_back("tags", pilot_info.tags, DB::Queries::ComparisonOp::ALL);
      }

      // NOTE(vformato): only keep fields that the pilot will actually need... This alleviates load on the DB
      json projectionOpt = R"({"_id":0, "dataset":0, "jobName":0, "status":0, "tags":0, "user":0})"_json;

      m_logger->trace("{} Pilot {} has no jobs available in cache. Querying DB...", logPrefix, pilot_info.uuid);

      auto maybe_query_result = m_frontDB->RunQuery(DB::Queries::Find{
          .collection = "jobs",
          .options = {.limit = static_cast<unsigned int>(query_limit)},
          .match = matches,
          .filter = projectionOpt,
      });

      if (!maybe_query_result) {
        m_logger->error("{} Failed to query frontend DB for jobs: {}", logPrefix, maybe_query_result.error().Message());
        m_claimedJobs[pilot_info.uuid] = ClaimedJob{
            .claimed = true,
            .job = R"({"sleep": true})"_json,
        };
        continue;
      }

      const auto &query_result = maybe_query_result.value();
      if (query_result.empty()) {
        m_logger->trace("{} Putting pilot {} to sleep", logPrefix, pilot_info.uuid);
        m_claimedJobs[pilot_info.uuid] = ClaimedJob{
            .claimed = true,
            .job = R"({"sleep": true})"_json,
        };
        continue;
      }

      std::ranges::for_each(query_result, [&](const auto &job) {
        if (!std::ranges::any_of(m_claimedJobs,
                                 [&job](const auto &p_it) { return p_it.second.job["hash"] == job["hash"]; })) {
          queried_job_pool[to_string(job["task"])].push_back(job);
        }
      });

      if (auto maybe_active_task = get_active_task(activeTasks); maybe_active_task) {
        const auto &selected_task = maybe_active_task.value();
        // take job from the queue for this pilot
        const auto job = queried_job_pool[selected_task].front();
        queried_job_pool[selected_task].pop_front();

        m_logger->trace("{} Pilot {} has jobs available in active task {}: selected job {}", logPrefix, pilot_info.uuid,
                        selected_task, to_string_view(job["hash"]));

        m_claimedJobs[pilot_info.uuid] = ClaimedJob{.job = job};
        --query_limit;
      }
    }

    if (m_claimedJobs.empty()) {
      continue;
    }

    // NOTE(vformato): Now we set all the jobs that we assigned to the pilots as Claimed
    std::vector<DB::Queries::Query> writeOps;
    auto wops_view = std::views::transform(
        m_claimedJobs | std::views::filter([](const auto &p_it) { return p_it.second.job.contains("hash"); }),
        [](auto p_it) {
          const auto &[pilotUuid, cjob] = p_it;
          DB::Queries::Updates update_action{
              {"status", magic_enum::enum_name(JobStatus::Claimed)},
              {"pilotUuid", pilotUuid},
              {"retries", 1, DB::Queries::UpdateOp::INC},
              {"lastUpdate", Utils::CurrentTimeToMillisSinceEpoch()},
          };
          return DB::Queries::Update{
              .collection = "jobs",
              .match = {{"hash", cjob.job["hash"]}},
              .update = update_action,
          };
        });
    std::ranges::copy(wops_view, std::back_inserter(writeOps));

    if (writeOps.empty()) {
      continue;
    }

    m_logger->trace("Updating {} jobs to Claimed status", writeOps.size());

    auto maybe_update_result = m_frontDB->BulkWrite("jobs", writeOps);
    while (!maybe_update_result) {
      m_logger->error("Failed to update jobs to claimed: {}", maybe_update_result.error().Message());

      std::this_thread::sleep_for(std::chrono::milliseconds(50));

      m_logger->debug("Retrying claimed jobs update...");
      maybe_update_result = m_frontDB->BulkWrite("jobs", writeOps);
    }

    std::ranges::for_each(m_claimedJobs, [](auto &cjob) { cjob.second.claimed = true; });

  } while (m_exitSignalFuture.wait_for(coolDown) == std::future_status::timeout);
}

ErrorOr<void> Director::UpdateJobStatus(std::string_view pilotUuid, std::string_view hash, std::string_view task,
                                        JobStatus status) {
  auto maybe_pilotInfo = GetPilotInfo(pilotUuid);
  if (!maybe_pilotInfo.has_value()) {
    return Error{std::errc::invalid_argument, fmt::format("Unknown pilot {}", pilotUuid)};
  }

  const auto &pilotInfo = maybe_pilotInfo.value();

  auto pilotTasks = pilotInfo.tasks;
  if (std::find(begin(pilotTasks), end(pilotTasks), task) == end(pilotTasks)) {
    return Error{std::errc::invalid_argument,
                 fmt::format("Pilot {} is not allowed to work on task {}", pilotUuid, task)};
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

  return outcome::success();
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
        m_logger->error("Failed to write job updates: {}", write_result.assume_error().Message());
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

  m_activePilots[to_string(pilotUuid)] = PilotInfo{to_string(pilotUuid), result.validTasks, tags};

  auto query_result = TRY(m_frontDB->RunQuery(DB::Queries::Insert{
      .collection = "pilots",
      .documents = {query},
  }));

  return result;
}

ErrorOr<void> Director::UpdateHeartBeat(std::string_view pilotUuid) {
  m_heartbeatUpdates.emplace(std::string{pilotUuid}, std::chrono::system_clock::now());
  return outcome::success();
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
      if (auto maybe_pilotInfo = GetPilotInfo(uuid); !maybe_pilotInfo)
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
        m_logger->error("Failed to write heartbeat updates: {}", write_result.assume_error().Message());
      }
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

    auto queryResult = pilot_query_result.assume_value();

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

      if (!job_query_result.assume_value().empty()) {
        json job = std::move(job_query_result.assume_value().front());
        m_logger->debug("Dead pilot {} had a running job ({}), setting to Error...", to_string_view(pilot["uuid"]),
                        to_string_view(job["hash"]));

        auto update_result = UpdateJobStatus(to_string_view(pilot["uuid"]), to_string_view(job["hash"]),
                                             to_string_view(job["task"]), JobStatus::Error);
        if (!update_result) {
          m_logger->error("Failed to update job status for job {}", to_string_view(job["hash"]));
          continue;
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
    std::ranges::transform(query_result.assume_value(), std::back_inserter(writeOps), [](const auto &job) {
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

ErrorOr<Director::PilotInfo> Director::GetPilotInfo(std::string_view uuid) {
  if (auto uuidString = std::string{uuid}; m_activePilots.find(uuidString) != end(m_activePilots)) {
    return m_activePilots[uuidString];
  } else {
    PilotInfo result;
    result.uuid = uuid;

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

ErrorOr<std::string> Director::Summary(const std::string &user) const {
  // auto handle = m_frontPoolHandle->DBHandle();

  json summary = json::array({});

  DB::Queries::Matches matches{{"user", user}};
  auto tasksQueryResult = TRY(m_frontDB->RunQuery(DB::Queries::Distinct{
      .collection = "jobs",
      .field = "task",
      .match = matches,
  }));

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

  return summary.dump();
}

ErrorOr<std::string> Director::QueryBackDB(QueryOperation operation, const json &match, const json &option) const {
  m_logger->debug("QueryBackDB: {} {} {}", magic_enum::enum_name(operation), match.dump(), option.dump());

  switch (operation) {
  case QueryOperation::UpdateOne: {
    auto matches = TRY(PMS::DB::Queries::ToMatches(match));
    auto update_action = TRY(PMS::DB::Queries::ToUpdates(option));
    update_action.emplace_back("lastUpdate", Utils::CurrentTimeToMillisSinceEpoch());

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
    update_action.emplace_back("lastUpdate", Utils::CurrentTimeToMillisSinceEpoch());

    auto result = TRY(m_frontDB->RunQuery(DB::Queries::Update{
        .collection = "jobs",
        .match = matches,
        .update = update_action,
    }));

    return fmt::format("Matched {} jobs. Updated {} jobs", result["matched_count"].get<size_t>(),
                       result["modified_count"].get<size_t>());
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

    return fmt::format("Deleted {} jobs", result["deleted_count"].get<size_t>());
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
