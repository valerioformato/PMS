// external dependencies
#include <fmt/format.h>

// our headers
#include "db/PoolHandle.h"

namespace PMS::DB {
PoolHandle::PoolHandle(std::string_view dbhost, std::string_view dbname)
    : m_pool{mongocxx::uri{fmt::format("mongodb://{}/{}", dbhost, dbname)}}, m_dbName{dbname} {}

PoolHandle::PoolHandle(std::string_view dbhost, std::string_view dbname, std::string_view dbuser,
                       std::string_view dbpwd)
    : m_pool{mongocxx::uri{fmt::format("mongodb://{}:{}@{}/{}", dbuser, dbpwd, dbhost, dbname)}}, m_dbName{dbname} {}
} // namespace PMS::DB