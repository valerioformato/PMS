// c++ headers
#include <thread>

// external headers
#include <fmt/format.h>
#include <spdlog/spdlog.h>

// our headers
#include "pilot/Worker.h"

namespace PMS {
namespace Pilot {
void Worker::Start(const std::string &user, const std::string &task) {

  bool work_done = false;

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

    auto query_result = m_dbhandle->DB()["jobs"].find_one(filter.view());
    if (query_result) {
      spdlog::info("Worker: got a new job");
    } else {
      spdlog::debug("Worker: no jobs, sleep for 1s");
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