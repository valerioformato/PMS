// c++ headers
#include <thread>

// external headers
#include <boost/process.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <fmt/format.h>
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
void Worker::Start(const std::string &user, const std::string &task) {

  // TODO: implement mechanism to decide when a task is actually
  // finished
  bool work_done = false;
  // bool work_done = true;

  // generate a pilot uuid
  boost::uuids::uuid uuid = boost::uuids::random_generator()();

  HeartBeat hb{uuid, m_poolHandle};

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

      // let's check if executable is a shell script
      if (executable.length() > 3 && executable.substr(executable.length() - 3, 3) == ".sh") {
        arguments.insert(begin(arguments), executable);
        executable = "sh";
      }

      std::string jobStdout = "/dev/null", jobStderr = "/dev/null", jobStdin = "/dev/null";
      try {
        if (job["stdout"] != "")
          jobStdout = job["stdout"];
        if (job["stderr"] != "")
          jobStderr = job["stderr"];
        if (job["stdin"] != "")
          jobStdin = job["stdin"];
      } catch (...) {
        spdlog::error("Worker: Job stdin/stdout/stderr not defined");
      }

      spdlog::info("Worker: Spawning process");
      if (arguments.empty())
        spdlog::info("Worker:  - {}", executable);
      else
        spdlog::info("Worker:  - {} {}", executable, fmt::join(arguments, " "));

      spdlog::trace("{} ({}, {}, {})", bp::search_path(executable), jobStdin, jobStdout, jobStderr);

      std::error_code procError;
      bp::child proc(bp::search_path(executable), arguments, bp::std_out > jobStdout, bp::std_err > jobStderr,
                     bp::std_in < jobStdin, bp::shell, procError);

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

    } else {
      spdlog::trace("Worker: no jobs, sleep for 1s");
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    if (work_done)
      break;
  }
}
} // namespace Pilot
} // namespace PMS