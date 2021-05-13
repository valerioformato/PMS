#ifndef PMS_DB_DBHANDLE_H
#define PMS_DB_DBHANDLE_H

// external dependencies
// shitload of mongodb driver headers
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

// our headers
#include "common/Job.h"

namespace PMS {
namespace DB {
class DBHandle {
public:
  DBHandle(mongocxx::pool &pool, std::string dbname) : m_poolEntry{pool.acquire()}, m_dbname{std::move(dbname)} {}

  mongocxx::collection operator[](bsoncxx::string::view_or_value name) const { return (*m_poolEntry)[m_dbname][name]; };

  void UpdateJobStatus(std::string hash, JobStatus status);

private:
  mongocxx::pool::entry m_poolEntry;
  std::string m_dbname;
};
} // namespace DB
} // namespace PMS

#endif