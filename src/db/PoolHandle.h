#ifndef PMS_DB_POOLHANDLE_H
#define PMS_DB_POOLHANDLE_H

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
#include "DBHandle.h"

namespace PMS {
namespace DB {
class PoolHandle {
public:
  PoolHandle(const std::string &dbhost, const std::string &dbname, const std::string &dbuser, const std::string &dbpwd);

  ::PMS::DB::DBHandle DBHandle() { return ::PMS::DB::DBHandle{m_pool, m_dbName}; }
  mongocxx::instance &Instance() { return m_mongo_instance; }

private:
  static mongocxx::instance m_mongo_instance;
  mongocxx::pool m_pool;
  std::string m_dbName;
};
} // namespace DB
} // namespace PMS
#endif
