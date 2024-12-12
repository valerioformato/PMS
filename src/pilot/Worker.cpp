// c++ headers
#include <filesystem>
#include <fstream>
#include <ranges>
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
#include "common/JsonUtils.h"
#include "pilot/HeartBeat.h"
#include "pilot/PilotInfo.h"
#include "pilot/Worker.h"

using json = nlohmann::json;
namespace bp = boost::process;
namespace fs = std::filesystem;

using namespace PMS::JsonUtils;

namespace PMS::Pilot {

ErrorOr<void> Worker::Register(const Info &info) {
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

  std::string response = TRY(m_wsConnection->Send(req.dump()));

  json reply;
  try {
    reply = json::parse(response);
  } catch (const std::exception &e) {
    return Error{std::make_error_code(std::errc::io_error), e.what()};
  }

  return reply["validTasks"].size() > 0 ? ErrorOr<void>{outcome::success()}
                                        : ErrorOr<void>{Error{std::make_error_code(std::errc::permission_denied),
                                                              "No valid tasks for this pilot"}};
}

void Worker::Start() {
  m_workerThread = std::thread{&Worker::MainLoop, this};
  m_jobUpdateThread = std::thread{&Worker::SendJobUpdates, this};
}

void Worker::Stop() { m_workerThread.join(); }

void Worker::Kill() {
  m_workerState = State::EXIT;
  m_exitSignal.set_value();
  m_jobProcess.terminate();
}

void Worker::UpdateJobStatus(const std::string &hash, const std::string &task, JobStatus status) {
  json request;
  request["command"] = "p_updateJobStatus";
  request["pilotUuid"] = boost::uuids::to_string(m_uuid);
  request["hash"] = hash;
  request["task"] = task;
  request["status"] = magic_enum::enum_name(status);

  m_queuedJobUpdates.push(request);
}

void Worker::SendJobUpdates() {
  while (true) {
    if (m_queuedJobUpdates.empty()) {
      if (m_workerState == State::EXIT) {
        return;
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
      }
    }

    json request = m_queuedJobUpdates.front();

    auto maybe_reply = m_wsConnection->Send(request.dump());
    if (!maybe_reply) {
      spdlog::error("{}", maybe_reply.error().Message());
    } else if (maybe_reply.assume_value() == "Ok"sv) {
      m_queuedJobUpdates.pop();
    } else {
      spdlog::error("Unexpected server reply: {}", maybe_reply.assume_value());
    }
  }
}

void Worker::MainLoop() {
  if (m_maxJobs < std::numeric_limits<decltype(m_maxJobs)>::max()) {
    spdlog::debug("Starting worker for {} jobs...", m_maxJobs);
  }

  constexpr auto maxWaitTime = std::chrono::minutes(10);
  auto sleepTime = std::chrono::seconds(1);

  HeartBeat hb{m_uuid, m_wsClient->PersistentConnection()};
  unsigned long int doneJobs = 0;

  auto startTime = std::chrono::system_clock::now();
  auto lastJobFinished = std::chrono::system_clock::now();

  // FIXME: This should be a deque probably
  std::deque<json> queued_job_updates;

  // main loop
  while (true) {

    if (m_workerState == State::EXIT) {
      break;
    }

    json request;
    request["command"] = "p_claimJob";
    request["pilotUuid"] = boost::uuids::to_string(m_uuid);
    spdlog::trace("{}", request.dump(2));

    auto response = m_wsConnection->Send(request.dump());
    if (!response) {
      spdlog::error("{}", response.assume_error().Message());
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

    json job;
    try {
      job = json::parse(response.assume_value());
      m_workerState = State::JOB_ACQUIRED;
    } catch (const std::exception &e) {
      spdlog::error("{}", e.what());
      continue;
    }

    if (job.contains("finished")) {
      spdlog::info("Worker: no jobs available. Exiting...");
      m_workerState = State::EXIT;
    } else if (job.contains("sleep")) {
      sleepTime = std::chrono::minutes(1);
      spdlog::info("Worker: sleeping for {} seconds", sleepTime);
      m_workerState = State::SLEEP;
    }

    if (!job.empty() && m_workerState == State::JOB_ACQUIRED) {
      spdlog::info("Worker: got a new job");
      spdlog::trace("Job: {}", job.dump(2));

      // prepare a sandbox directory for the job
      fs::path wdPath = fs::absolute(fs::path{fmt::format("job_{}", std::string(job["hash"]))});
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

        std::ranges::transform(exeArgs, std::back_inserter(arguments), [](const auto &arg) { return to_string(arg); });
      } catch (...) {
        spdlog::error("Worker: Error fetching the executable and its arguments");
        break;
      }

      jobSTDIO jobIO;
      if (job.contains("stdout") && job["stdout"] != "")
        jobIO.stdout = fmt::format("{}/{}", wdPath.string(), to_string_view(job["stdout"]));
      if (job.contains("stderr") && job["stderr"] != "")
        jobIO.stderr = fmt::format("{}/{}", wdPath.string(), to_string_view(job["stderr"]));
      if (job.contains("stdin") && job["stdin"] != "")
        jobIO.stdin = fmt::format("{}/{}", wdPath.string(), to_string_view(job["stdin"]));

      // TODO: factorize env preparation in helper function
      if (!job["env"].empty()) {
        EnvInfoType envType = GetEnvType(to_string(job["env"]["type"]));
        switch (envType) {
        case EnvInfoType::Script:
          try {
            if (!job["env"].contains("args")) {
              shellScript.print(". {}\n", fs::canonical(to_string(job["env"]["file"])).string());
            } else {
              std::vector<std::string> dummy;
              auto scriptArgs = job["env"]["args"];
              std::ranges::transform(scriptArgs, std::back_inserter(dummy),
                                     [](const auto &arg) { return to_string(arg); });
              std::string scriptWithArgs =
                  fmt::format("{} {}\n", fs::canonical(to_string(job["env"]["file"])).string(), fmt::join(dummy, " "));
              shellScript.print(". {}", scriptWithArgs);
            }
          } catch (const fs::filesystem_error &e) {
            spdlog::error("{}", e.what());
            continue;
          }
          break;
        case EnvInfoType::NONE:
          spdlog::error("Invalid env type {} is not supported", to_string_view(job["env"]["type"]));
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

        UpdateJobStatus(to_string(job["hash"]), to_string(job["task"]), JobStatus::InboundTransfer);

        auto result = ftQueue.Process();
        if (!result) {
          spdlog::error("File transfer process failed: {}", result.assume_error().Message());
          UpdateJobStatus(to_string(job["hash"]), to_string(job["task"]), JobStatus::InboundTransferError);
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
      UpdateJobStatus(to_string(job["hash"]), to_string(job["task"]), JobStatus::Running);

      m_jobProcess.wait();

      spdlog::debug("Process exited with code {}", m_jobProcess.exit_code());

      auto dump_file = [](const std::string_view filePath) {
        std::ifstream file(filePath.data());
        if (file.is_open()) {
          std::string line;
          while (std::getline(file, line)) {
            std::cout << line << std::endl;
          }
          file.close();
        }
      };

      JobStatus nextJobStatus;
      if (procError || m_jobProcess.exit_code()) {
        spdlog::error("Worker: Job exited with an error. Dumping stderr:");
        dump_file(jobIO.stderr);
        nextJobStatus = JobStatus::Error;
      } else if (m_workerState == State::EXIT) {
        spdlog::warn("Worker: Requested termination. Marking job as failed...");
        nextJobStatus = JobStatus::Error;
      } else {
        spdlog::info("Worker: Job done");
        nextJobStatus = JobStatus::Done;
      }

      // check for outbound file transfers
      if (job.contains("output")) {
        FileTransferQueue ftQueue;
        auto fts = ParseFileTransferRequest(FileTransferType::Outbound, job["output"], wdPath.string());
        for (const auto &ftJob : fts) {
          ftQueue.Add(ftJob);
        }

        UpdateJobStatus(to_string(job["hash"]), to_string(job["task"]), JobStatus::OutboundTransfer);

        // Call the Process method and check the return value
        auto result = ftQueue.Process();
        if (!result) {
          spdlog::error("File transfer process failed: {}", result.assume_error().Message());
          nextJobStatus = JobStatus::OutboundTransferError;
        } else {
          spdlog::trace("Outbound transfers completed");
        }
      }

      UpdateJobStatus(to_string(job["hash"]), to_string(job["task"]), nextJobStatus);

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
                         [&element](const auto &tag) { return tag == element["tag"].get<std::string>(); });
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

    std::string fileName = to_string(doc["file"]);
    bool hasWildcard = fileName.find("*") != std::string::npos;
    if (hasWildcard)
      FileTransferQueue::ProcessWildcards(fileName);

    auto protocol = magic_enum::enum_cast<FileTransferProtocol>(doc["protocol"].get<std::string_view>());
    if (!protocol.has_value()) {
      spdlog::error("Invalid file transfer protocol: {}", to_string_view(doc["protocol"]));
    }

    std::string remotePath =
        type == FileTransferType::Inbound ? to_string(doc["source"]) : to_string(doc["destination"]);

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
