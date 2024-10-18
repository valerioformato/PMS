#pragma once

#include <variant>

#include "common/JsonUtils.h"
#include "common/Utils.h"

namespace PMS::DB::Queries {

#define GENERATE_QUERY_MEMBERS(NAME)                                                                                   \
  bool operator==(const NAME &other) const = default;                                                                  \
  std::string collection{};                                                                                            \
  Options options{};

struct Options {
  unsigned int limit{0};
  unsigned int skip{0};
  bool bypass_document_validation{false};
  bool upsert{false};

  bool operator==(const Options &other) const = default;
};

struct Find {
  GENERATE_QUERY_MEMBERS(Find)

  json match;
  json filter;
};

struct Insert {
  GENERATE_QUERY_MEMBERS(Insert)

  std::vector<json> documents;
};

struct Update {
  GENERATE_QUERY_MEMBERS(Update)

  json match;
  json update;
};

struct Delete {
  GENERATE_QUERY_MEMBERS(Delete)

  json match;
};

struct Count {
  GENERATE_QUERY_MEMBERS(Count)

  json match;
};

struct Aggregate {
  GENERATE_QUERY_MEMBERS(Aggregate)

  json pipeline;
};

#undef GENERATE_QUERY_MEMBERS

using Query = std::variant<Find, Insert, Update, Delete, Count, Aggregate>;

inline constexpr std::string to_string(const Query &query) {
  return std::visit(PMS::Utils::overloaded{
                        [](const Find &q) { return fmt::format("Find: {}, {}", q.match.dump(), q.filter.dump()); },
                        [](const Insert &q) { return fmt::format("Insert: {}", json(q.documents).dump()); },
                        [](const Update &q) { return fmt::format("Update: {}, {}", q.match.dump(), q.update.dump()); },
                        [](const Delete &q) { return fmt::format("Delete: {}", q.match.dump()); },
                        [](const Count &q) { return fmt::format("Count: {}", q.match.dump()); },
                        [](const Aggregate &q) { return fmt::format("Aggregate: {}", q.pipeline.dump()); }},
                    query);
}
} // namespace PMS::DB::Queries
