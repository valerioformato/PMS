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
  if (!m_config.tags.empty())
    req["tags"] = m_config.tags;

  json reply;
  try {
    reply = json::parse(m_wsConnection->Send(req.dump()));
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

  HeartBeat hb{m_uuid, m_wsConnection};
  unsigned long int doneJobs = 0;

  auto lastJobFinished = std::chrono::system_clock::now();

  std::vector<json> abandonedJobs;

  // main loop
  while (true) {

    json request;
    request["command"] = "p_claimJob";
    request["pilotUuid"] = boost::uuids::to_string(m_uuid);
    spdlog::trace("{}", request.dump(2));

    json job;
    try {
      job = json::parse(m_wsConnection->Send(request.dump()));
    } catch (const Connection::FailedConnectionException &e) {
      if (!hb.IsAlive())
        break;
    }

    // Here we check if we had abandoned jobs that have not been updated on the server and we process them
    if (!abandonedJobs.empty()) {
      std::for_each(begin(abandonedJobs), end(abandonedJobs),
                    [this](json &job) { UpdateJobStatus(job["hash"], job["task"], JobStatus::Error); });
      abandonedJobs.clear();
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
        FileTransferQueue ftQueue;
        auto fts = ParseFileTransferRequest(FileTransferType::Inbound, job["input"], wdPath.string());
        for (const auto &ftJob : fts) {
          ftQueue.Add(ftJob);
        }
        ftQueue.Process();
        spdlog::trace("Inbound tranfers completed?");
      }

      fs::path exePath{executable};
      if (!fs::exists(exePath)) {
        spdlog::error("Cannot find file {}", exePath.string());
      }
      spdlog::trace("{} ({}, {}, {})", exePath.string(), jobIO.stdin, jobIO.stdout, jobIO.stderr);

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
      bp::child proc(bp::search_path("sh"), shellScriptPath.filename().string(), bp::std_out > jobIO.stdout,
                     bp::std_err > jobIO.stderr, bp::std_in < jobIO.stdin, bp::start_dir(wdPath.string()), bp::shell,
                     procError);

      // set status to "Running"
      if (!UpdateJobStatus(job["hash"], job["task"], JobStatus::Running)) {
        spdlog::error("Can't reach server while trying to set job as Running.");
        // NOTE(vformato):  in this case we do nothing. We hope that while the process runs the connection is recovered.
      }

      proc.wait();

      if (procError) {
        spdlog::error("Worker: Job exited with an error: {}", procError.message());
        if (!UpdateJobStatus(job["hash"], job["task"], JobStatus::Error)) {
          spdlog::error("Can't reach server while trying to set job as Error. Abandoning job...");

          // NOTE(vformato): Here we actually need to handle the failure. We push the job to a queue of "abandoned"
          // jobs, and we'll communicate to the server as soon as the connection becomes available...
          abandonedJobs.push_back(job);
          continue;
        }
      } else {
        spdlog::info("Worker: Job done");
        if (!UpdateJobStatus(job["hash"], job["task"], JobStatus::Done)) {
          spdlog::error("Can't reach server while trying to set job as Done. Abandoning job...");

          // NOTE(vformato): Here we actually need to handle the failure. We push the job to a queue of "abandoned"
          // jobs, and we'll communicate to the server as soon as the connection becomes available...
          abandonedJobs.push_back(job);
          continue;
        }

        // check for outbound file transfers
        if (job.contains("output")) {
          FileTransferQueue ftQueue;
          auto fts = ParseFileTransferRequest(FileTransferType::Outbound, job["output"], wdPath.string());
          for (const auto &ftJob : fts) {
            ftQueue.Add(ftJob);
          }
          ftQueue.Process();
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
        spdlog::trace("Worker: no jobs for {:%M:%S}... Exiting now.",
                      std::chrono::duration_cast<std::chrono::seconds>(maxWaitTime));
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
  auto reply = m_wsConnection->Send(request.dump());

  return reply == "Ok";
}

std::vector<FileTransferInfo> Worker::ParseFileTransferRequest(FileTransferType type, const json &request,
                                                               std::string_view currentPath) {
  std::vector<FileTransferInfo> result;

  if (!request.contains("files")) {
    spdlog::error(R"(No "files" field present in file transfer request.)");
    return result;
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