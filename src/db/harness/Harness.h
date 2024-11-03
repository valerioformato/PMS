#pragma once

#include <spdlog/spdlog.h>

#include "common/JsonUtils.h"
#include "db/backends/Backend.h"
#include "db/queries/Queries.h"

namespace PMS::DB {
using QueryResult = json;

class Harness {
public:
  Harness(std::unique_ptr<Backend> backend) : m_backend(std::move(backend)) {}

  [[nodiscard]] ErrorOr<void> Connect() { return m_backend->Connect(); }
  [[nodiscard]] ErrorOr<void> Connect(std::string_view user, std::string_view password) {
    return m_backend->Connect(user, password);
  }

  [[nodiscard]] ErrorOr<void> SetupIfNeeded() { return m_backend->SetupIfNeeded(); }

  [[nodiscard]] ErrorOr<QueryResult> RunQuery(Queries::Query query) {
    m_logger->trace("Running query {}", PMS::DB::Queries::to_string(query));
    return m_backend->RunQuery(query);
  };

  [[nodiscard]] ErrorOr<QueryResult> BulkWrite(std::string_view table_or_collection,
                                               std::vector<Queries::Query> queries) {
    m_logger->trace("Running bulk write on collection {} ({} ops)", table_or_collection, queries.size());
    return m_backend->BulkWrite(table_or_collection, queries);
  }

  static std::shared_ptr<spdlog::logger> m_logger;

private:
  std::unique_ptr<Backend> m_backend;
};
} // namespace PMS::DB
