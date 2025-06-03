#include "db/queries/Update.h"

namespace PMS::DB::Queries {

auto ToUpdates(const json &match_json) -> ErrorOr<DB::Queries::Updates> {
  DB::Queries::Updates updates;
  for (const auto &[full_op, field_update] : match_json.items()) {
    auto op_name = full_op.substr(1);
    std::ranges::transform(op_name, op_name.begin(), ::toupper);
    auto op = magic_enum::enum_cast<DB::Queries::UpdateOp>(op_name);
    if (!op.has_value()) {
      return Error(std::errc::invalid_argument, fmt::format("Invalid comparison operator: {}", full_op));
    }

    for (const auto &[key, value] : field_update.items()) {
      updates.emplace_back(UpdateAction{.key = key, .value = value, .op = op.value()});
    }
  }

  return updates;
};

} // namespace PMS::DB::Queries
