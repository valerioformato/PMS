#pragma once

#include <common/JsonUtils.h>
#include <variant>

namespace PMS::DB::Queries {

#define GENERATE_QUERY_SPECIAL_MEMBERS(NAME)                                                                           \
  NAME() = default;                                                                                                    \
  NAME(const NAME &) = default;                                                                                        \
  NAME(NAME &&) = default;                                                                                             \
  NAME &operator=(const NAME &) = default;                                                                             \
  NAME &operator=(NAME &&) = default;                                                                                  \
  ~NAME() = default;                                                                                                   \
  bool operator==(const NAME &other) const = default;                                                                  \
  std::string collection;                                                                                              \
  Options options;

struct Options {
  unsigned int limit{0};
  unsigned int skip{0};
  bool bypass_document_validation{false};

  bool operator==(const Options &other) const = default;
};

struct Find {
  GENERATE_QUERY_SPECIAL_MEMBERS(Find)

  json match;
  json filter;
};

struct Insert {
  GENERATE_QUERY_SPECIAL_MEMBERS(Insert)

  std::vector<json> documents;
};

struct Update {
  GENERATE_QUERY_SPECIAL_MEMBERS(Update)

  json match;
  json update;
};

struct Delete {
  GENERATE_QUERY_SPECIAL_MEMBERS(Delete)

  json match;
};

struct Count {
  GENERATE_QUERY_SPECIAL_MEMBERS(Count)

  json match;
};

struct Aggregate {
  GENERATE_QUERY_SPECIAL_MEMBERS(Aggregate)

  json pipeline;
};

using Query = std::variant<Find, Insert, Update, Delete, Count, Aggregate>;
} // namespace PMS::DB::Queries
