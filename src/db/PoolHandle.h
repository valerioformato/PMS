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

namespace PMS::DB {
class PoolHandle {
public:
  PoolHandle(std::string_view dbhost, std::string_view dbname);
  PoolHandle(std::string_view dbhost, std::string_view dbname, std::string_view dbuser, std::string_view dbpwd);

  ::PMS::DB::DBHandle DBHandle() { return ::PMS::DB::DBHandle{m_pool, m_dbName}; }
  static mongocxx::instance &Instance() { return m_mongo_instance; }

private:
  static mongocxx::instance m_mongo_instance;
  mongocxx::pool m_pool;
  std::string m_dbName;
};
} // namespace PMS::DB
#endif
