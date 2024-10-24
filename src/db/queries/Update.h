#pragma once

#include <magic_enum.hpp>

#include "common/JsonUtils.h"

namespace PMS::DB::Queries {
enum class UpdateOp { SET, INC, MUL, MIN, MAX, CURRENT_DATE };

struct UpdateAction {
  std::string key;
  json value = "{}"_json;
  UpdateOp op{UpdateOp::SET};

  std::string dump() const { return fmt::format(R"({{"{}" {} {}}})", key, magic_enum::enum_name(op), value.dump()); }

  bool operator==(const UpdateAction &other) const {
    return key == other.key && value == other.value && op == other.op;
  }
};
} // namespace PMS::DB::Queries