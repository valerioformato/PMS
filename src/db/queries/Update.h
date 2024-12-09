#pragma once

#include <magic_enum.hpp>

#include "common/JsonUtils.h"
#include "common/Utils.h"

namespace PMS::DB::Queries {
enum class UpdateOp { SET, INC, MUL, MIN, MAX, PUSH };

struct UpdateAction {
  std::string key;
  json value = "{}"_json;
  UpdateOp op{UpdateOp::SET};

  std::string dump() const { return fmt::format(R"({{"{}" {} {}}})", key, magic_enum::enum_name(op), value.dump()); }

  bool operator==(const UpdateAction &other) const {
    return key == other.key && value == other.value && op == other.op;
  }
};

using Updates = std::vector<UpdateAction>;
ErrorOr<DB::Queries::Updates> ToUpdates(const json &match_json);

} // namespace PMS::DB::Queries
