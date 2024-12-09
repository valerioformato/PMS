#pragma once

#include <string_view>

#include <mongocxx/instance.hpp>
#include <mongocxx/model/write.hpp>
#include <mongocxx/pool.hpp>

#include "db/backends/Backend.h"

namespace PMS::DB {
class MongoDBBackend : public Backend {
public:
  MongoDBBackend(std::string_view dbhost, std::string_view dbname);

  virtual ~MongoDBBackend() = default;

  [[nodiscard]] virtual ErrorOr<void> Connect() override;
  [[nodiscard]] virtual ErrorOr<void> Connect(std::string_view user, std::string_view password) override;

  [[nodiscard]] ErrorOr<void> SetupIfNeeded() override;

  [[nodiscard]] ErrorOr<QueryResult> RunQuery(Queries::Query query) override;
  [[nodiscard]] ErrorOr<QueryResult> BulkWrite(std::string_view collection,
                                               std::vector<Queries::Query> queries) override;

  [[nodiscard]] static json MatchesToJson(const Queries::Matches &matches);
  [[nodiscard]] static json UpdatesToJson(const Queries::Updates &updates);

  [[nodiscard]] static ErrorOr<mongocxx::model::write> QueryToWriteOp(const Queries::Query &query);

private:
  std::string m_dbhost;
  std::string m_dbname;

  static mongocxx::instance m_mongo_instance;
  std::unique_ptr<mongocxx::pool> m_pool;
};
} // namespace PMS::DB
