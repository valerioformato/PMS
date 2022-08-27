// c++ headers
#include <string_view>

// external headers
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "common/JsonUtils.h"
#include "db/DBHandle.h"

using json = nlohmann::json;

namespace PMS::DB {
void DBHandle::SetupDBCollections() {
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
  if (!(*m_poolEntry)[m_dbname].has_collection("tasks")) {
    spdlog::debug("Creating indexes for the \"tasks\" collection");
    mongocxx::options::index index_options{};
    // task name is a unique index
    index_options.unique(true);
    (*m_poolEntry)[m_dbname]["tasks"].create_index(make_document(kvp("name", 1)), index_options);
  }
  if (!(*m_poolEntry)[m_dbname].has_collection("pilots")) {
    spdlog::debug("Creating indexes for the \"pilots\" collection");
    mongocxx::options::index index_options{};
    // pilot uuid is a unique index
    index_options.unique(true);
    (*m_poolEntry)[m_dbname]["pilots"].create_index(make_document(kvp("uuid", 1)), index_options);
  }
}
} // namespace PMS::DB
