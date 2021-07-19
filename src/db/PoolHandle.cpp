// external dependencies
#include <fmt/format.h>

// our headers
#include "db/PoolHandle.h"

namespace PMS::DB {
PoolHandle::PoolHandle(const std::string &dbhost, const std::string &dbname)
    : m_pool{mongocxx::uri{fmt::format("mongodb://{}/{}", dbhost, dbname)}}, m_dbName{dbname} {}

PoolHandle::PoolHandle(const std::string &dbhost, const std::string &dbname, const std::string &dbuser,
                       const std::string &dbpwd)
    : m_pool{mongocxx::uri{fmt::format("mongodb://{}:{}@{}/{}", dbuser, dbpwd, dbhost, dbname)}}, m_dbName{dbname} {}
} // namespace PMS::DB