// c++ headers
#include <thread>

// external headers
#include <boost/process.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <fmt/format.h>
#include <fmt/os.h>
#include <fmt/ostream.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "common/JsonUtils.h"
#include "pilot/HeartBeat.h"
#include "pilot/Worker.h"

using json = nlohmann::json;
namespace bp = boost::process;

namespace PMS {
namespace Pilot {
void Worker::Start(const std::string &user, const std::string &task, unsigned long int maxJobs) {

  // TODO: implement mechanism to decide when a task is actually
  // finished
  bool work_done = false;
  // bool work_done = true;

  // generate a pilot uuid
  boost::uuids::uuid uuid = boost::uuids::random_generator()();

  HeartBeat hb{uuid, m_poolHandle};

  unsigned long int doneJobs;

  // main loop
  while (true) {
    json filter;
    if (!user.empty())
      filter["user"] = user;
    if (!task.empty())
      filter["task"] = task;
    filter["status"] = JobStatusNames[JobStatus::Pending];

    json updateAction;
    updateAction["$set"]["status"] = JobStatusNames[JobStatus::Claimed];
    updateAction["$set"]["pilotUuid"] = boost::uuids::to_string(uuid);

    DB::DBHandle dbHandle = m_poolHandle->DBHandle();

    // get new job and mark it as claimed
    auto query_result =
        dbHandle["jobs"].find_one_and_update(JsonUtils::json2bson(filter), JsonUtils::json2bson(updateAction));

    if (query_result) {
      json job = JsonUtils::bson2json(query_result.value());
      spdlog::info("Worker: got a new job");

      spdlog::trace("Job: {}", job.dump(2));

      // prepare a sandbox directory for the job
      boost::filesystem::path wdPath{fmt::format("job_{}", std::string(job["hash"]))};
      boost::filesystem::create_directory(wdPath);

      // make the shell script executable
      boost::filesystem::path shellScriptPath = wdPath / "pilot_task.sh";
      auto shellScript = fmt::output_file(shellScriptPath.string());
      boost::filesystem::permissions(shellScriptPath, boost::filesystem::owner_all | boost::filesystem::group_read);

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
      if (job["stdout"] != "")
        finalIO.stdout = job["stdout"];
      if (job["stderr"] != "")
        finalIO.stderr = job["stderr"];
      if (job["stdin"] != "")
        finalIO.stdin = job["stdin"];

      // local IO to the working directory, will copy later to the final location
      jobSTDIO localIO{finalIO.stdin.empty() ? "/dev/null" : finalIO.stdin,
                       fmt::format("{}/stdout.txt", wdPath.string()), fmt::format("{}/stderr.txt", wdPath.string())};

      if (!job["env"].empty()) {
        EnvInfoType envType = GetEnvType(job["env"]["type"]);
        switch (envType) {
        case EnvInfoType::Script:
          shellScript.print(". {}\n", boost::filesystem::canonical(job["env"]["file"]).string());
          break;
        case EnvInfoType::NONE:
          spdlog::error("Invalid env type {} is not supported", job["env"]["type"]);
        default:
          break;
        }
      }

      boost::filesystem::path exePath{executable};
      spdlog::trace("{} ({}, {}, {})", exePath.string(), finalIO.stdin, finalIO.stdout, finalIO.stderr);

      spdlog::info("Worker: Spawning process");
      std::string executableWithArgs;
      if (arguments.empty()) {
        executableWithArgs = fmt::format("{}", boost::filesystem::canonical(exePath).string());
      } else {
        executableWithArgs =
            fmt::format("{} {}", boost::filesystem::canonical(exePath).string(), fmt::join(arguments, " "));
      }
      spdlog::info("Worker:  - {}", executableWithArgs);

      shellScript.print("{}\n", executableWithArgs);

      // close the script file before execution, or the child will silently fail
      shellScript.close();

      std::error_code procError;
      bp::child proc(bp::search_path("sh"), shellScriptPath.filename(), bp::std_out > localIO.stdout,
                     bp::std_err > localIO.stderr, bp::std_in < localIO.stdin, bp::start_dir(wdPath), bp::shell,
                     procError);

      // set status to "Running"
      dbHandle.UpdateJobStatus(job["hash"], JobStatus::Running);

      proc.wait();

      if (procError) {
        spdlog::trace("procerr: {} ({})", procError.message(), procError.value());
        spdlog::error("Worker: Job exited with an error: {}", procError.message());
        dbHandle.UpdateJobStatus(job["hash"], JobStatus::Error);
      } else {
        spdlog::info("Worker: Job done");
        dbHandle.UpdateJobStatus(job["hash"], JobStatus::Done);
      }

      // remove temporary sandbox directory
      boost::filesystem::remove(wdPath);

      if (++doneJobs == maxJobs)
        work_done = true;

    } else {
      spdlog::trace("Worker: no jobs, sleep for 1s");
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    if (work_done)
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

} // namespace Pilot
} // namespace PMS