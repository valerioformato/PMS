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
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

namespace PMS {
namespace DB {
class DBHandle {
public:
  DBHandle(const std::string& dbhost, const std::string& dbname, const std::string& dbuser, const std::string& dbpwd);

  mongocxx::instance &Instance() { return m_mongo_instance; }

private:
  static mongocxx::instance m_mongo_instance;
  mongocxx::client m_mongo_client;
};
} // namespace DB
} // namespace PMS
#endif
