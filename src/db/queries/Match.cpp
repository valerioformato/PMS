#include <algorithm>

#include "db/queries/Match.h"

namespace PMS::DB::Queries {

auto ToMatches(const json &match_json) -> ErrorOr<DB::Queries::Matches> {
  DB::Queries::Matches matches;
  for (const auto &[key, value] : match_json.items()) {
    if (value.contains("$in")) {
      matches.emplace_back(key, value["$in"], DB::Queries::ComparisonOp::IN);
    } else if (value.is_object()) {
      const auto &[op, val] = value.items().begin();
      if (op.front() == '$') {
        auto opname = op.substr(1);
        std::ranges::transform(opname, opname.begin(), ::toupper);
        auto comp = magic_enum::enum_cast<DB::Queries::ComparisonOp>(opname);
        if (comp.has_value()) {
          matches.emplace_back(key, val, comp.value());
        } else {
          return outcome::failure(boost::system::errc::invalid_argument);
        }
      }
    } else {
      matches.emplace_back(key, value, DB::Queries::ComparisonOp::EQ);
    }
  }

  return outcome::success(matches);
};

} // namespace PMS::DB::Queries