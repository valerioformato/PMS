#pragma once

#include <spdlog/spdlog.h>

#include "common/JsonUtils.h"
#include "db/Queries.h"
#include "db/backends/Backend.h"

namespace PMS::DB {
using QueryResult = json;

class Harness {
public:
  Harness(std::unique_ptr<Backend> backend) : m_backend(std::move(backend)) {}

  ErrorOr<void> Connect() { return m_backend->Connect(); }

  ErrorOr<void> SetupIfNeeded() { return m_backend->SetupIfNeeded(); }

  ErrorOr<QueryResult> RunQuery(Queries::Query query) {
    spdlog::trace("[Harness] Running query {}", PMS::DB::Queries::to_string(query));
    return m_backend->RunQuery(query);
  };

private:
  std::unique_ptr<Backend> m_backend;
};
} // namespace PMS::DB
