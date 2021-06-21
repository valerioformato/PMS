// external headers
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "common/JsonUtils.h"
#include "db/DBHandle.h"

using json = nlohmann::json;

namespace PMS {
namespace DB {
void DBHandle::UpdateJobStatus(const std::string &hash, JobStatus status) const {
  json jobFilter;
  try {
    jobFilter["hash"] = hash;
  } catch (...) {
    spdlog::error("DBHandle::UpdateJobStatus Job has no hash, THIS SHOULD NEVER HAPPEN!");
    return;
  }

  json jobUpdateAction;
  jobUpdateAction["$set"]["status"] = JobStatusNames[status];
  jobUpdateAction["$currentDate"]["lastUpdate"] = true;

  switch (status) {
  case JobStatus::Running:
    jobUpdateAction["$currentDate"]["startTime"] = true;
    break;
  case JobStatus::Error:
  case JobStatus::Done:
    jobUpdateAction["$currentDate"]["finishTime"] = true;
    break;
  default:
    break;
  }

  this->operator[]("jobs").update_one(JsonUtils::json2bson(jobFilter), JsonUtils::json2bson(jobUpdateAction));
}

void DBHandle::SetupJobIndexes() {
  using bsoncxx::builder::basic::kvp;
  using bsoncxx::builder::basic::make_document;

  if (!(*m_poolEntry)[m_dbname].has_collection("jobs")) {
    spdlog::debug("Creating indexes for the \"jobs\" collection");
    mongocxx::options::index index_options{};
    // hash is a unique index
    index_options.unique(true);
    (*m_poolEntry)[m_dbname]["jobs"].create_index(make_document(kvp("hash", 1)), index_options);

    // task is not a unique index :)
    index_options.unique(false);
    (*m_poolEntry)[m_dbname]["jobs"].create_index(make_document(kvp("task", 1)), index_options);
  }
}
} // namespace DB
} // namespace PMS