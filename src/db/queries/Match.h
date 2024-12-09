#pragma once

#include <magic_enum.hpp>

#include "common/JsonUtils.h"
#include "common/Utils.h"

namespace PMS::DB::Queries {
enum class ComparisonOp { EQ, NE, GT, GTE, LT, LTE, IN, TYPE, ALL, EXISTS };

struct Match {
  std::string key;
  json value = "{}"_json;
  ComparisonOp op{ComparisonOp::EQ};

  std::string dump() const { return fmt::format(R"({{"{}" {} {}}})", key, magic_enum::enum_name(op), value.dump()); }

  bool operator==(const Match &other) const { return key == other.key && value == other.value && op == other.op; }
};

using Matches = std::vector<Match>;
ErrorOr<DB::Queries::Matches> ToMatches(const json &match_json);
} // namespace PMS::DB::Queries
