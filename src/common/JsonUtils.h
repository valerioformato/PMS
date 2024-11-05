//
// Created by Valerio Formato on 04/06/21.
//

#ifndef PMS_JSONUTILS_H
#define PMS_JSONUTILS_H

#include <nlohmann/json.hpp>

#include <functional>

using json = nlohmann::json;

namespace PMS::JsonUtils {
constexpr std::string_view to_string_view(const json &j) { return j.get<std::string_view>(); }
constexpr std::string to_string(const json &j) { return j.get<std::string>(); }
} // namespace PMS::JsonUtils
#endif // PMS_JSONUTILS_H
