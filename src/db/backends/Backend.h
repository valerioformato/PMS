#pragma once

#include "common/JsonUtils.h"
#include "common/Utils.h"
#include "db/Queries.h"

namespace PMS::DB {
using QueryResult = json;

class Backend {
public:
  virtual ErrorOr<void> Connect() = 0;

  virtual ErrorOr<QueryResult> RunQuery(Queries::Query query) = 0;
};
}