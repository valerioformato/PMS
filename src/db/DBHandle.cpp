// external headers
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "db/DBHandle.h"

using json = nlohmann::json;

namespace PMS {
namespace DB {
void DBHandle::UpdateJobStatus(std::string hash, JobStatus status) {
  json jobFilter;
  try {
    jobFilter["hash"] = hash;
  } catch (...) {
    spdlog::error("DBHandle::UpdateJobStatus Job has no hash, THIS SHOULD NEVER HAPPEN!");
    return;
  }

  json jobUpdateRunningAction;
  jobUpdateRunningAction["$set"]["status"] = JobStatusNames[status];
  this->operator[]("jobs").update_one(bsoncxx::from_json(jobFilter.dump()),
                                      bsoncxx::from_json(jobUpdateRunningAction.dump()));
}
} // namespace DB
} // namespace PMS