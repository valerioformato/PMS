//
// Created by Valerio Formato on 04/06/21.
//

#ifndef PMS_JSONUTILS_H
#define PMS_JSONUTILS_H

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/json.hpp>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace PMS {
namespace JsonUtils {
inline json bson2json(bsoncxx::document::view bsonDoc) { return json::parse(bsoncxx::to_json(bsonDoc)); }
inline bsoncxx::document::value json2bson(const json& jsonDoc) { return bsoncxx::from_json(jsonDoc.dump()); }
} // namespace JsonUtils
} // namespace PMS
#endif // PMS_JSONUTILS_H
