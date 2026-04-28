#pragma once

#include <spdlog/spdlog.h>

#include <stdexec/execution.hpp>

#include "common/JsonUtils.h"
#include "common/Types/Error.h"
#include "db/backends/Backend.h"
#include "db/queries/Queries.h"

namespace PMS::DB {
using QueryResult = json;

class Harness {
public:
  explicit Harness(std::unique_ptr<Backend> backend) : m_backend(std::move(backend)) {}

  [[nodiscard]] ErrorOr<void> Connect() const { return m_backend->Connect(); }
  [[nodiscard]] ErrorOr<void> Connect(std::string_view user, std::string_view password) const {
    return m_backend->Connect(user, password);
  }

  [[nodiscard]] ErrorOr<void> SetupIfNeeded() const { return m_backend->SetupIfNeeded(); }

  [[nodiscard]] ErrorOr<QueryResult> RunQuery(Queries::Query query) const {
    m_logger->trace("Running query {}", PMS::DB::Queries::to_string(query));
    return m_backend->RunQuery(query);
  };

  [[nodiscard]] stdexec::sender auto RunQuery(stdexec::scheduler auto scheduler, Queries::Query query) const {
    m_logger->trace("Running query {}", PMS::DB::Queries::to_string(query));
    return stdexec::on(scheduler, stdexec::just(std::move(query)) |
                                      stdexec::then([this](Queries::Query q) { return RunQuery(q); }));
  };

  [[nodiscard]] ErrorOr<QueryResult> BulkWrite(std::string_view table_or_collection,
                                               std::vector<Queries::Query> queries) const {
    m_logger->trace("Running bulk write on collection {} ({} ops)", table_or_collection, queries.size());
    return m_backend->BulkWrite(table_or_collection, queries);
  }

  [[nodiscard]] stdexec::sender auto BulkWrite(stdexec::scheduler auto scheduler, std::string_view table_or_collection,
                                               std::vector<Queries::Query> queries) const {
    m_logger->trace("Running bulk write on collection {} ({} ops)", table_or_collection, queries.size());
    return stdexec::on(scheduler, stdexec::just(std::string(table_or_collection), std::move(queries)) |
                                      stdexec::then([this](std::string tbl, std::vector<Queries::Query> q) {
                                        return m_backend->BulkWrite(tbl, q);
                                      }));
  };

  static std::shared_ptr<spdlog::logger> m_logger;

private:
  std::unique_ptr<Backend> m_backend;
};
} // namespace PMS::DB
