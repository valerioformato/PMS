//
// Created by Valerio Formato on 04/06/21.
//

#ifndef PMS_JSONUTILS_H
#define PMS_JSONUTILS_H

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/value.hpp>
#include <nlohmann/json.hpp>

#include <functional>

using json = nlohmann::json;

namespace PMS::JsonUtils {
constexpr std::string_view to_sv(const json &j) { return j.get<std::string_view>(); }
constexpr std::string to_s(const json &j) { return j.get<std::string>(); }
inline json bson2json(bsoncxx::document::view bsonDoc) { return json::parse(bsoncxx::to_json(bsonDoc)); }
inline bsoncxx::document::value json2bson(const json &jsonDoc) { return bsoncxx::from_json(jsonDoc.dump()); }

using Predicate = std::function<bool(const bsoncxx::document::element &el)>;
inline bsoncxx::document::value filter(bsoncxx::document::view doc, const Predicate &predicate) {
  using namespace bsoncxx::builder;

  basic::document builder;
  for (auto &el : doc) {
    if (!predicate(el)) {
      builder.append(basic::kvp(el.key(), el.get_value()));
    }
  }
  return builder.extract();
}

} // namespace PMS::JsonUtils
#endif // PMS_JSONUTILS_H
