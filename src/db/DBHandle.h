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

namespace PMS {
namespace DB {
class DBHandle {
public:
  DBHandle(mongocxx::pool &pool, std::string dbname) : m_poolEntry{pool.acquire()}, m_dbname{std::move(dbname)} {}

  mongocxx::database DB() { return (*m_poolEntry)[m_dbname]; };

private:
  mongocxx::pool::entry m_poolEntry;
  std::string m_dbname;
};
} // namespace DB
} // namespace PMS

#endif