#pragma once

#include <common/JsonUtils.h>
#include <db/harness/Queries.h>

#include <boost/outcome.hpp>
namespace outcome = boost::outcome_v2;

namespace PMS {
template <typename T> using ErrorOr = outcome::result<T>;
}

namespace PMS::DB {
using QueryResult = json;

class Backend {
public:
  virtual ErrorOr<void> Connect() = 0;
};

class Harness {
public:
  Harness(std::unique_ptr<Backend> backend) : m_backend(std::move(backend)) {}

  ErrorOr<void> Connect() { return m_backend->Connect(); }

  ErrorOr<QueryResult> RunQuery(Queries::Query query) { return QueryResult{}; };

private:
  std::unique_ptr<Backend> m_backend;
};
} // namespace PMS::DB
