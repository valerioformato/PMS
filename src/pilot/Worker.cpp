// c++ headers
#include <filesystem>
#include <thread>

// external headers
#include <boost/process.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/os.h>
#include <magic_enum.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "common/Job.h"
#include "pilot/HeartBeat.h"
#include "pilot/Worker.h"

using json = nlohmann::json;
namespace bp = boost::process;
namespace fs = std::filesystem;

namespace PMS::Pilot {

bool Worker::Register() {
  // generate a pilot uuid
  m_uuid = boost::uuids::random_generator()();

  json req;
  req["command"] = "p_registerNewPilot";
  req["pilotUuid"] = boost::uuids::to_string(m_uuid);
  req["user"] = m_config.user;
  req["tasks"] = json::array({});
  for (const auto &task : m_config.tasks) {
    req["tasks"].emplace_back(json::object({{"name", task.first}, {"token", task.second}}));
  }

  json reply;
  try {
    reply = json::parse(m_wsClient->Send(req));
  } catch (const Connection::FailedConnectionException &e) {
    spdlog::error("Failed connection to server...");
    return false;
  }

  fmt::print("{}\n", reply.dump(2));
  return reply["validTasks"].size() > 0;
}

void Worker::Start(unsigned long int maxJobs) {

  // TODO: implement mechanism to decide when a task is actually
  // finished
  bool exit = false;
  // bool exit = true;

  constexpr auto maxWaitTime = std::chrono::minutes(10);

  HeartBeat hb{m_uuid, m_wsClient};
  unsigned long int doneJobs = 0;

  auto lastJobFinished = std::chrono::system_clock::now();

  // main loop
  while (true) {

    json request;
    request["command"] = "p_claimJob";
    request["pilotUuid"] = boost::uuids::to_string(m_uuid);

    json job;
    try {
      job = json::parse(m_wsClient->Send(request));
    } catch (const Connection::FailedConnectionException &e) {
      if (!hb.IsAlive())
        break;
    }

    if (!job.empty()) {
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

      jobSTDIO finalIO;
      if (job.contains("stdout") && job["stdout"] != "")
        finalIO.stdout = job["stdout"];
      if (job.contains("stderr") && job["stderr"] != "")
        finalIO.stderr = job["stderr"];
      if (job.contains("stdin") && job["stdin"] != "")
        finalIO.stdin = job["stdin"];

      // local IO to the working directory, will copy later to the final location
      jobSTDIO localIO{finalIO.stdin.empty() ? "/dev/null" : finalIO.stdin,
                       fmt::format("{}/stdout.txt", wdPath.string()), fmt::format("{}/stderr.txt", wdPath.string())};

      // TODO: factorize env preparation in helper function
      if (!job["env"].empty()) {
        EnvInfoType envType = GetEnvType(job["env"]["type"]);
        switch (envType) {
        case EnvInfoType::Script:
          try {
            shellScript.print(". {}\n", fs::canonical(job["env"]["file"]).string());
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
        auto fts = ParseFileTransferRequest(FileTransferType::Inbound, job["input"]);
        for (const auto &ftJob : fts) {
          spdlog::info("Attempting to transfer {} from {} ({} protocol)", ftJob.fileName, ftJob.remotePath,
                       magic_enum::enum_name(ftJob.protocol));
          if (FileTransfer(ftJob)) {
            spdlog::info("Transfer succeeded");
          } else {
            spdlog::error("Transfer failed");
          }
        }
      }

      fs::path exePath{executable};
      if(!fs::exists(exePath)){
        spdlog::error("Cannot find file {}", exePath.string());
      }
      spdlog::trace("{} ({}, {}, {})", exePath.string(), finalIO.stdin, finalIO.stdout, finalIO.stderr);

      spdlog::info("Worker: Spawning process");
      std::string executableWithArgs;
      if (arguments.empty()) {
        executableWithArgs = fmt::format("{}", fs::canonical(exePath).string());
      } else {
        executableWithArgs = fmt::format("{} {}", fs::canonical(exePath).string(), fmt::join(arguments, " "));
      }
      spdlog::info("Worker:  - {}", executableWithArgs);

      shellScript.print("{}\n", executableWithArgs);

      // close the script file before execution, or the child will silently fail
      shellScript.close();

      std::error_code procError;
      bp::child proc(bp::search_path("sh"), shellScriptPath.filename().string(), bp::std_out > localIO.stdout,
                     bp::std_err > localIO.stderr, bp::std_in < localIO.stdin, bp::start_dir(wdPath.string()),
                     bp::shell, procError);

      // set status to "Running"
      UpdateJobStatus(job["hash"], job["task"], JobStatus::Running);

      proc.wait();

      if (procError) {
        spdlog::error("Worker: Job exited with an error: {}", procError.message());
        UpdateJobStatus(job["hash"], job["task"], JobStatus::Error);
      } else {
        spdlog::info("Worker: Job done");
        UpdateJobStatus(job["hash"], job["task"], JobStatus::Done);

        // check for outbound file transfers
        if (job.contains("output")) {
          auto fts = ParseFileTransferRequest(FileTransferType::Outbound, job["output"]);
          for (const auto &ftJob : fts) {
            spdlog::info("Attempting to transfer {} to {} ({} protocol)", ftJob.fileName, ftJob.remotePath,
                         magic_enum::enum_name(ftJob.protocol));
            if (FileTransfer(ftJob)) {
              spdlog::info("Transfer succeeded");
            } else {
              spdlog::error("Transfer failed");
            }
          }
        }
      }

      // remove temporary sandbox directory
      fs::remove_all(wdPath);

      lastJobFinished = std::chrono::system_clock::now();

      if (++doneJobs == maxJobs)
        exit = true;

    } else {
      std::this_thread::sleep_for(std::chrono::seconds(1));

      auto delta = std::chrono::system_clock::now() - lastJobFinished;
      if (delta > maxWaitTime) {
        exit = true;
      } else {
        spdlog::trace("Worker: no jobs, been waiting for {:%M:%S}...",
                      std::chrono::duration_cast<std::chrono::seconds>(delta));
        continue;
      }
    }

    if (exit)
      break;
  }
}

Worker::EnvInfoType Worker::GetEnvType(const std::string &envName) {
  auto it = std::find_if(begin(m_envInfoNames), end(m_envInfoNames),
                         [&envName](const auto &_pair) { return _pair.second == envName; });

  if (it == end(m_envInfoNames))
    return EnvInfoType::NONE;

  return it->first;
}

bool Worker::UpdateJobStatus(const std::string &hash, const std::string &task, JobStatus status) {
  json request;
  request["command"] = "p_updateJobStatus";
  request["pilotUuid"] = boost::uuids::to_string(m_uuid);
  request["hash"] = hash;
  request["task"] = task;
  request["status"] = magic_enum::enum_name(status);
  auto reply = m_wsClient->Send(request);

  return reply == "Ok";
}

} // namespace PMS::Pilot