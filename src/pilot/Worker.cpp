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

namespace PMS {
namespace Pilot {
void Worker::Start(const std::string &user, const std::string &task) {

  bool work_done = false;

  // generate a pilot uuid
  boost::uuids::uuid uuid = boost::uuids::random_generator()();

  HeartBeat hb{uuid, m_poolHandle};

  // main loop
  // TODO: run in a thread
  while (true) {
    bsoncxx::builder::stream::document filter;
    if (user != "") {
      filter << "user" << user;
    }
    if (task != "") {
      filter << "task" << task;
    }

    DB::DBHandle dbHandle = m_poolHandle->DBHandle();

    auto query_result = dbHandle["jobs"].find_one(filter.view());
    if (query_result) {
      spdlog::info("Worker: got a new job");

      json job = json::parse(bsoncxx::to_json(query_result.value()));

      spdlog::trace("Job: {}", job.dump(2));

      // break for now
      std::this_thread::sleep_for(std::chrono::seconds(10));
      break;
      // but we should prepare and  start the process here

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