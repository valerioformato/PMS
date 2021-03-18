// external dependencies
#include <fmt/format.h>

// our headers
#include "db/DBHandle.h"

namespace PMS {
namespace DB {
DBHandle::DBHandle(const std::string &dbhost, const std::string &dbname, const std::string &dbuser,
                   const std::string &dbpwd)
    : m_mongo_client{mongocxx::uri{fmt::format("mongodb://{}:{}@{}/{}", dbuser, dbpwd, dbhost, dbname)}},
      m_mongo_db{m_mongo_client[dbname]} {}
} // namespace DB
} // namespace PMS