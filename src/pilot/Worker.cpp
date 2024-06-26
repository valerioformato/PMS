// c++ headers
#include <filesystem>
#include <thread>

// external headers
#include <boost/uuid/uuid_io.hpp>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/os.h>
#include <fmt/ranges.h>
#include <magic_enum.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "PMSVersion.h"
#include "common/Job.h"
#include "pilot/HeartBeat.h"
#include "pilot/PilotInfo.h"
#include "pilot/Worker.h"

using json = nlohmann::json;
namespace bp = boost::process;
namespace fs = std::filesystem;

namespace PMS::Pilot {

bool Worker::Register(const Info &info) {
  json req;

  m_uuid = info.uuid;

  // get host info
  req["host"]["hostname"] = info.hostname;
  req["host"]["ip"] = info.ip;
  req["host"]["os_version"] = info.os_version;

  // prepare command
  req["command"] = "p_registerNewPilot";
  req["pilotUuid"] = boost::uuids::to_string(info.uuid);
  req["user"] = m_config.user;
  req["tasks"] = json::array({});
  for (const auto &task : m_config.tasks) {
    req["tasks"].emplace_back(json::object({{"name", task.first}, {"token", task.second}}));
  }
  if (!m_config.tags.empty())
    req["tags"] = m_config.tags;

  json reply;
  try {
    reply = json::parse(m_wsConnection->Send(req.dump()));
  } catch (const Connection::FailedConnectionException &e) {
    spdlog::error("Failed connection to server...");
    return false;
  }

  return reply["validTasks"].size() > 0;
}

void Worker::Start() { m_workerThread = std::thread{&Worker::MainLoop, this}; }

void Worker::Stop() { m_workerThread.join(); }

void Worker::Kill() {
  m_workerState = State::EXIT;
  m_exitSignal.set_value();
  m_jobProcess.terminate();
}

bool Worker::UpdateJobStatus(const std::string &hash, const std::string &task, JobStatus status) {
  json request;
  request["command"] = "p_updateJobStatus";
  request["pilotUuid"] = boost::uuids::to_string(m_uuid);
  request["hash"] = hash;
  request["task"] = task;
  request["status"] = magic_enum::enum_name(status);

  try {
    auto reply = m_wsConnection->Send(request.dump());
    return reply == "Ok"sv;
  } catch (const Connection::FailedConnectionException &e) {
    return false;
  }
}

void Worker::MainLoop() {
  if (m_maxJobs < std::numeric_limits<decltype(m_maxJobs)>::max()) {
    spdlog::debug("Starting worker for {} jobs...", m_maxJobs);
  }

  constexpr auto maxWaitTime = std::chrono::minutes(10);
  auto sleepTime = std::chrono::seconds(1);

  HeartBeat hb{m_uuid, m_wsConnection};
  unsigned long int doneJobs = 0;

  auto startTime = std::chrono::system_clock::now();
  auto lastJobFinished = std::chrono::system_clock::now();

  // FIXME: This should be a deque probably
  std::deque<json> queued_job_updates;

  auto handleJobStatusChange = [&queued_job_updates, this](const json &job, JobStatus status) {
    if (!UpdateJobStatus(job["hash"], job["task"], status)) {
      spdlog::error("Can't reach server while trying to set job status as {}. Queueing up job status update for later.",
                    magic_enum::enum_name(status));

      // NOTE(vformato): Here we actually need to handle the failure. We push the job to a queue of "to-be-updated"
      // jobs, and we'll communicate to the server as soon as the connection becomes available...
      json &queued_job_update = queued_job_updates.emplace_back(job);
      queued_job_update["status"] = magic_enum::enum_name(status);
      return false;
    }

    return true;
  };

  // main loop
  while (true) {

    if (m_workerState == State::EXIT) {
      break;
    }

    json request;
    request["command"] = "p_claimJob";
    request["pilotUuid"] = boost::uuids::to_string(m_uuid);
    spdlog::trace("{}", request.dump(2));

    json job;
    try {
      job = json::parse(m_wsConnection->Send(request.dump()));
      m_workerState = State::JOB_ACQUIRED;
    } catch (const Connection::FailedConnectionException &e) {
      if (!hb.IsAlive()) {
        spdlog::warn("No connection to server and heartbeat is not alive. Exiting...");
        m_workerState = State::EXIT;
      } else {
        spdlog::warn("No connection to server. Waiting...");
        m_workerState = State::WAIT;
        std::this_thread::sleep_for(std::chrono::seconds(10));
      }
      continue;
    }

    // Here we check if we had abandoned jobs that have not been updated on the server and we process them
    // NOTE: at this point, we should have a connection to the server
    if (!queued_job_updates.empty()) {
      while (!queued_job_updates.empty()) {
        auto &job = queued_job_updates.front();

        JobStatus status = magic_enum::enum_cast<JobStatus>(job["status"].get<std::string_view>()).value();
        spdlog::debug("Sending queued job status update {} to {}", job["hash"], job["status"]);
        handleJobStatusChange(job, status);

        queued_job_updates.pop_front();
      }
    }

    if (job.contains("finished")) {
      m_workerState = State::EXIT;
    } else if (job.contains("sleep")) {
      sleepTime = std::chrono::minutes(1);
      m_workerState = State::SLEEP;
    }

    if (!job.empty() && m_workerState == State::JOB_ACQUIRED) {
      spdlog::info("Worker: got a new job");
      spdlog::trace("Job: {}", job.dump(2));

      // prepare a sandbox directory for the job
      fs::path wdPath{fmt::format("job_{}", std::string(job["hash"]))};
      fs::create_directory(wdPath);

      // make the shell script executable
      fs::path shellScriptPath = wdPath / "pilot_task.sh";
      auto shellScript = fmt::output_file(shellScriptPath.string());
      fs::permissions(shellScriptPath, fs::perms::owner_all | fs::perms::group_read);

      shellScript.print("#! /bin/bash\n");

      std::string executable;
      std::vector<std::string> arguments;
      try {
        executable = job["executable"].get<std::string>();
        auto exeArgs = job["exe_args"];
        std::copy(exeArgs.begin(), exeArgs.end(), std::back_inserter(arguments));
      } catch (...) {
        spdlog::error("Worker: Error fetching the executable and its arguments");
        break;
      }

      jobSTDIO jobIO;
      if (job.contains("stdout") && job["stdout"] != "")
        jobIO.stdout = fmt::format("{}/{}", wdPath.string(), job["stdout"]);
      if (job.contains("stderr") && job["stderr"] != "")
        jobIO.stderr = fmt::format("{}/{}", wdPath.string(), job["stderr"]);
      if (job.contains("stdin") && job["stdin"] != "")
        jobIO.stdin = fmt::format("{}/{}", wdPath.string(), job["stdin"]);

      // TODO: factorize env preparation in helper function
      if (!job["env"].empty()) {
        EnvInfoType envType = GetEnvType(job["env"]["type"]);
        switch (envType) {
        case EnvInfoType::Script:
          try {
            if (!job["env"].contains("args")) {
              shellScript.print(". {}\n", fs::canonical(job["env"]["file"]).string());
            } else {
              std::vector<std::string> dummy;
              auto scriptArgs = job["env"]["args"];
              std::copy(scriptArgs.begin(), scriptArgs.end(), std::back_inserter(dummy));
              std::string scriptWithArgs =
                  fmt::format("{} {}\n", fs::canonical(job["env"]["file"]).string(), fmt::join(dummy, " "));
              shellScript.print(". {}", scriptWithArgs);
            }
          } catch (const fs::filesystem_error &e) {
            spdlog::error("{}", e.what());
            continue;
          }
          break;
        case EnvInfoType::NONE:
          spdlog::error("Invalid env type {} is not supported", job["env"]["type"]);
        default:
          break;
        }
      }

      // check for inbound file transfers
      if (job.contains("input")) {
        FileTransferQueue ftQueue;
        auto fts = ParseFileTransferRequest(FileTransferType::Inbound, job["input"], wdPath.string());
        for (const auto &ftJob : fts) {
          ftQueue.Add(ftJob);
        }

        handleJobStatusChange(job, JobStatus::InboundTransfer);

        bool success = ftQueue.Process();
        if (!success) {
          spdlog::error("File transfer process failed");
          handleJobStatusChange(job, JobStatus::InboundTransferError);
          m_workerState = State::WAIT;

          // remove temporary sandbox directory
          fs::remove_all(wdPath);

          continue;
        }
        spdlog::trace("Inbound transfers completed");
      }

      spdlog::trace("{} ({}, {}, {})", executable, jobIO.stdin, jobIO.stdout, jobIO.stderr);

      spdlog::info("Worker: Spawning process");
      std::string executableWithArgs;
      if (arguments.empty()) {
        executableWithArgs = fmt::format("{}", executable);
      } else {
        executableWithArgs = fmt::format("{} {}", executable, fmt::join(arguments, " "));
      }
      spdlog::info("Worker:  - {}", executableWithArgs);

      // set the shell to exit as soon as a command fails
      shellScript.print("set -e\n");

      // run the executable
      shellScript.print("{}\n", executableWithArgs);

      // close the script file before execution, or the child will silently fail
      shellScript.close();

      m_workerState = State::RUN;

      std::error_code procError;
      m_jobProcess = bp::child(bp::search_path("sh"), shellScriptPath.filename().string(), bp::std_out > jobIO.stdout,
                               bp::std_err > jobIO.stderr, bp::std_in < jobIO.stdin, bp::start_dir(wdPath.string()),
                               bp::shell, procError);

      // set status to "Running"
      handleJobStatusChange(job, JobStatus::Running);

      m_jobProcess.wait();

      spdlog::debug("Process exited with code {}", m_jobProcess.exit_code());

      JobStatus nextJobStatus;
      if (procError || m_jobProcess.exit_code()) {
        spdlog::error("Worker: Job exited with an error: {}", procError.message());
        nextJobStatus = JobStatus::Error;
      } else if (m_workerState == State::EXIT) {
        spdlog::warn("Worker: Requested termination. Marking job as failed...");
        nextJobStatus = JobStatus::Error;
      } else {
        spdlog::info("Worker: Job done");
        nextJobStatus = JobStatus::Done;
      }

      constexpr unsigned int max_transfer_tries{3};

      // check for outbound file transfers
      if (job.contains("output")) {
        FileTransferQueue ftQueue;
        auto fts = ParseFileTransferRequest(FileTransferType::Outbound, job["output"], wdPath.string());
        for (const auto &ftJob : fts) {
          ftQueue.Add(ftJob);
        }

        // UpdateJobStatus(job["hash"], job["task"], JobStatus::OutboundTransfer);
        handleJobStatusChange(job, JobStatus::OutboundTransfer);

        unsigned int file_transfer_tries{0};
        while (true) {
          // Call the Process method and check the return value
          bool success = ftQueue.Process();
          if (!success) {
            ++file_transfer_tries;

            spdlog::error("File transfer process failed");
            if (file_transfer_tries == max_transfer_tries) {
              nextJobStatus = JobStatus::OutboundTransferError;
              break;
            }

            // wait 3 seconds before retrying
            std::this_thread::sleep_for(std::chrono::seconds(3));
          } else {
            break;
          }
        }
      }

      handleJobStatusChange(job, nextJobStatus);

      // remove temporary sandbox directory
      fs::remove_all(wdPath);

      lastJobFinished = std::chrono::system_clock::now();
      auto delta = lastJobFinished - startTime;

      if (++doneJobs == m_maxJobs || std::chrono::duration_cast<decltype(m_maxTime)>(delta) > m_maxTime) {
        m_workerState = State::EXIT;
      }

    } else if (m_workerState != State::EXIT) {
      m_exitSignalFuture.wait_for(sleepTime);

      auto delta = std::chrono::system_clock::now() - lastJobFinished;
      if (delta > maxWaitTime && m_workerState == State::WAIT) {
        spdlog::trace("Worker: no jobs for {:%M:%S}... Exiting now.",
                      std::chrono::duration_cast<std::chrono::seconds>(maxWaitTime));
        m_workerState = State::EXIT;
      } else {
        spdlog::trace("Worker: no jobs, been waiting for {:%M:%S}...",
                      std::chrono::duration_cast<std::chrono::seconds>(delta));
        continue;
      }
    } else {
      m_exitSignal.set_value();
    }
  }
}

Worker::EnvInfoType Worker::GetEnvType(const std::string &envName) {
  auto it = std::find_if(begin(m_envInfoNames), end(m_envInfoNames),
                         [&envName](const auto &_pair) { return _pair.second == envName; });

  if (it == end(m_envInfoNames))
    return EnvInfoType::NONE;

  return it->first;
}

std::vector<FileTransferInfo> Worker::ParseFileTransferRequest(FileTransferType type, json request,
                                                               std::string_view currentPath) {
  std::vector<FileTransferInfo> result;

  if (request.is_array()) {
    // We have an array: this means output depends on the pilot tag
    auto tmpIt = std::find_if(request.begin(), request.end(), [this](const json &element) {
      return std::any_of(begin(m_config.tags), end(m_config.tags),
                         [&element](const auto &tag) { return tag == element["tag"]; });
    });

    // if there is a tag matching this pilot we parse the associated request
    if (tmpIt != request.end()) {
      request = *tmpIt;
    } else {
      throw std::runtime_error(fmt::format("No output tag matches this pilot tags ({})", m_config.tags));
    }
  }

  if (!request.contains("files")) {
    throw std::domain_error(R"(No "files" field present in file transfer request.)");
  }

  constexpr static std::array requiredFields{"file"sv, "protocol"sv};
  std::vector<std::string_view> additionalFields;
  switch (type) {
  case FileTransferType::Inbound:
    additionalFields = {"source"};
    break;
  case FileTransferType::Outbound:
    additionalFields = {"destination"};
    break;
  }

  for (const auto &doc : request["files"]) {
    for (const auto field : requiredFields) {
      if (!doc.contains(field)) {
        spdlog::error("Missing file transfer field: \"{}\"", field);
        continue;
      }
    }

    for (const auto &field : additionalFields) {
      if (!doc.contains(field)) {
        spdlog::error("Missing file transfer field: \"{}\"", field);
        continue;
      }
    }

    std::string fileName = doc["file"];
    bool hasWildcard = fileName.find("*") != std::string::npos;
    if (hasWildcard)
      FileTransferQueue::ProcessWildcards(fileName);

    auto protocol = magic_enum::enum_cast<FileTransferProtocol>(doc["protocol"].get<std::string_view>());
    if (!protocol.has_value()) {
      spdlog::error("Invalid file transfer protocol: {}", doc["protocol"]);
    }

    std::string remotePath = type == FileTransferType::Inbound ? doc["source"] : doc["destination"];

    FileTransferInfo ftInfo{type, protocol.value(), fileName, remotePath, std::string{currentPath}};
    if (hasWildcard) {
      for (const auto &expFile : FileTransferQueue::ExpandWildcard(ftInfo)) {
        result.push_back(FileTransferInfo{type, protocol.value(), expFile, remotePath, std::string{currentPath}});
      }
    } else {
      result.push_back(ftInfo);
    }
  }

  return result;
}

} // namespace PMS::Pilot
