#pragma once

#include "common/JsonUtils.h"
#include "common/Utils.h"
#include "db/queries/Queries.h"

namespace PMS::DB {
using QueryResult = json;

class Backend {
public:
  virtual ErrorOr<void> Connect() = 0;
  virtual ErrorOr<void> Connect(std::string_view user, std::string_view password) = 0;

  virtual ErrorOr<void> SetupIfNeeded() = 0;

  virtual ErrorOr<QueryResult> RunQuery(Queries::Query query) = 0;
  virtual ErrorOr<QueryResult> BulkWrite(std::string_view table_or_collection, std::vector<Queries::Query> queries) = 0;
};
} // namespace PMS::DB
