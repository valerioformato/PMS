// c++ headers
#include <thread>

// external headers
#include <boost/process.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "pilot/HeartBeat.h"
#include "pilot/Worker.h"

using json = nlohmann::json;
namespace bp = boost::process;

namespace PMS {
namespace Pilot {
void Worker::Start(const std::string &user, const std::string &task) {

  // TODO: implement mechanism to decide when a task is actually
  // finished
  // bool work_done = false;
  bool work_done = true;

  // generate a pilot uuid
  boost::uuids::uuid uuid = boost::uuids::random_generator()();

  HeartBeat hb{uuid, m_poolHandle};

  // main loop
  // TODO: run in a thread
  while (true) {
    json filter;
    if (user != "")
      filter["user"] = user;
    if (task != "")
      filter["task"] = task;
    filter["status"] = "Pending";

    DB::DBHandle dbHandle = m_poolHandle->DBHandle();

    auto query_result = dbHandle["jobs"].find_one(bsoncxx::from_json(filter.dump()));
    if (query_result) {
      spdlog::info("Worker: got a new job");

      json job = json::parse(bsoncxx::to_json(query_result.value()));

      spdlog::trace("Job: {}", job.dump(2));

      // break for now
      // std::this_thread::sleep_for(std::chrono::seconds(10));
      // break;
      // but we should prepare and  start the process here

      std::string executable;
      std::string arguments;
      try {
        executable = job["executable"].get<std::string>();
        auto exeArgs = job["exe_args"];
        arguments = std::accumulate(
            exeArgs.begin(), exeArgs.end(), std::string{},
            [](std::string &acc, const std::string &value) { return fmt::format("{} {}", acc, value); });
      } catch (...) {
        spdlog::error("Error fetching the executable and its arguments");
        break;
      }

      json jobFilter;
      try {
        jobFilter["hash"] = job["hash"];
      } catch (...) {
        spdlog::error("Job has no hash, THIS SHOULD NEVER HAPPEN!");
        break;
      }

      spdlog::info("Spawning process");
      spdlog::info(" - {} {}", executable, arguments);
      std::error_code procError;
      bp::child proc{bp::search_path(executable), arguments, procError};

      // set status to "Running"
      json jobUpdateRunningAction;
      jobUpdateRunningAction["$set"]["status"] = "Running";
      dbHandle["jobs"].update_one(bsoncxx::from_json(jobFilter.dump()),
                                  bsoncxx::from_json(jobUpdateRunningAction.dump()));

      proc.wait();

      // TODO: encapsulate status updating in DBHandle class?
      // Maybe using magic_enum this could get more elegant and typesafe
      if (procError) {
        spdlog::error("Job exited with an error: {}", procError.message());
        json jobUpdateErrorAction;
        jobUpdateErrorAction["$set"]["status"] = "Error";
        dbHandle["jobs"].update_one(bsoncxx::from_json(jobFilter.dump()),
                                    bsoncxx::from_json(jobUpdateRunningAction.dump()));
      } else {
        spdlog::info("Job done");
        json jobUpdateDoneAction;
        jobUpdateDoneAction["$set"]["status"] = "Done";
        dbHandle["jobs"].update_one(bsoncxx::from_json(jobFilter.dump()),
                                    bsoncxx::from_json(jobUpdateDoneAction.dump()));
      }

    } else {
      spdlog::trace("Worker: no jobs, sleep for 1s");
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    // do the actual work, fork and run the job

    if (work_done)
      break;
  }
}
} // namespace Pilot
} // namespace PMS