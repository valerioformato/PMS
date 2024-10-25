#include "db/queries/Update.h"

namespace PMS::DB::Queries {

auto ToUpdates(const json &match_json) -> ErrorOr<DB::Queries::Updates> {
  DB::Queries::Updates updates;
  for (const auto &[key, value] : match_json.items()) {
    if (value.is_object()) {
      const auto &[op, val] = value.items().begin();
      if (op.front() == '$') {
        auto opname = op.substr(1);
        std::ranges::transform(opname, opname.begin(), ::toupper);

        auto comp = magic_enum::enum_cast<DB::Queries::UpdateOp>(opname);
        if (comp.has_value()) {
          updates.emplace_back(key, val, comp.value());
        } else {
          return outcome::failure(boost::system::errc::invalid_argument);
        }
      }
    }
  }

  return outcome::success(updates);
};

} // namespace PMS::DB::Queries