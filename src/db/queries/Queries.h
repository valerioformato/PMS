#pragma once

#include <variant>

#include "common/JsonUtils.h"
#include "common/Utils.h"

#include "db/queries/Match.h"
#include "db/queries/Update.h"

namespace PMS::DB::Queries {
using OverriddenComparisons = std::unordered_map<std::string_view, ComparisonOp>;

#define GENERATE_QUERY_MEMBERS(NAME)                                                                                   \
  bool operator==(const NAME &other) const = default;                                                                  \
  static constexpr std::string_view name{#NAME};                                                                       \
  std::string collection{};                                                                                            \
  OverriddenComparisons comparisons{};                                                                                 \
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

  Matches match{};
  json filter = "{}"_json;
};

struct FindOneAndUpdate {
  GENERATE_QUERY_MEMBERS(FindOneAndUpdate)

  Matches match{};
  Updates update{};
  json filter = "{}"_json;
};

struct Insert {
  GENERATE_QUERY_MEMBERS(Insert)

  std::vector<json> documents;
};

struct Update {
  GENERATE_QUERY_MEMBERS(Update)

  Matches match;
  Updates update;
};

struct Delete {
  GENERATE_QUERY_MEMBERS(Delete)

  Matches match;
};

struct Count {
  GENERATE_QUERY_MEMBERS(Count)

  Matches match;
};

struct Aggregate {
  GENERATE_QUERY_MEMBERS(Aggregate)

  json pipeline;
};

#undef GENERATE_QUERY_MEMBERS

using Query = std::variant<Find, FindOneAndUpdate, Insert, Update, Delete, Count, Aggregate>;

inline std::string DumpMatches(const Matches &matches) {
  if (matches.empty()) {
    return "{}";
  }

  std::string result{"["};
  for (const auto &match : matches) {
    result += match.dump() + ", ";
  }
  result += "]";
  return result;
}

inline std::string DumpUpdates(const Updates &updates) {
  std::string result{"["};
  for (const auto &update : updates) {
    result += update.dump() + ", ";
  }
  result += "]";
  return result;
}

inline constexpr std::string to_string(const Query &query) {
  return std::visit(
      PMS::Utils::overloaded{
          [](const Find &q) { return fmt::format("Find: {}, {}", DumpMatches(q.match), q.filter.dump()); },
          [](const FindOneAndUpdate &q) {
            return fmt::format("FindOneAndUpdate: {}, {}", DumpMatches(q.match), DumpUpdates(q.update),
                               q.filter.dump());
          },
          [](const Insert &q) { return fmt::format("Insert: {}", json(q.documents).dump()); },
          [](const Update &q) { return fmt::format("Update: {}, {}", DumpMatches(q.match), DumpUpdates(q.update)); },
          [](const Delete &q) { return fmt::format("Delete: {}", DumpMatches(q.match)); },
          [](const Count &q) { return fmt::format("Count: {}", DumpMatches(q.match)); },
          [](const Aggregate &q) { return fmt::format("Aggregate: {}", q.pipeline.dump()); }},
      query);
}
} // namespace PMS::DB::Queries
