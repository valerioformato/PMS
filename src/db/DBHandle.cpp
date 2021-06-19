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

  json jobUpdateRunningAction;
  jobUpdateRunningAction["$set"]["status"] = JobStatusNames[status];
  this->operator[]("jobs").update_one(JsonUtils::json2bson(jobFilter), JsonUtils::json2bson(jobUpdateRunningAction));
}

void DBHandle::ClaimJob(const std::string &hash, const std::string &pilotUuid) const {
  json jobFilter;
  try {
    jobFilter["hash"] = hash;
  } catch (...) {
    spdlog::error("DBHandle::UpdateJobStatus Job has no hash, THIS SHOULD NEVER HAPPEN!");
    return;
  }

  json jobUpdateRunningAction;
  jobUpdateRunningAction["$set"]["status"] = JobStatusNames[JobStatus::Claimed];
  jobUpdateRunningAction["$set"]["pilotUuid"] = pilotUuid;
  this->operator[]("jobs").update_one(JsonUtils::json2bson(jobFilter), JsonUtils::json2bson(jobUpdateRunningAction));
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