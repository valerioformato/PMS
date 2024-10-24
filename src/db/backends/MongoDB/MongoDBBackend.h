#pragma once

#include <string_view>

#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>

#include "db/backends/Backend.h"

namespace PMS::DB {
class MongoDBBackend : public Backend {
public:
  MongoDBBackend(std::string_view dbhost, std::string_view dbname);

  virtual ~MongoDBBackend() = default;

  virtual ErrorOr<void> Connect() override;
  virtual ErrorOr<void> Connect(std::string_view user, std::string_view password) override;

  ErrorOr<void> SetupIfNeeded() override;

  ErrorOr<QueryResult> RunQuery(Queries::Query query) override;

  static json MatchesToJson(const Queries::Matches &matches);
  static json UpdatesToJson(const Queries::Updates &updates);

private:
  std::string m_dbhost;
  std::string m_dbname;

  static mongocxx::instance m_mongo_instance;
  std::unique_ptr<mongocxx::pool> m_pool;
};
} // namespace PMS::DB
