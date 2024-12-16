#include <ranges>

#include <magic_enum.hpp>
#include <spdlog/spdlog.h>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/value.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/exception/exception.hpp>

#include "MongoDBBackend.h"
#include "db/backends/MongoDB/MongoDBBackend.h"

namespace PMS::JsonUtils {
static json bson2json(bsoncxx::document::view bsonDoc) { return json::parse(bsoncxx::to_json(bsonDoc)); }
static bsoncxx::document::value json2bson(const json &jsonDoc) { return bsoncxx::from_json(jsonDoc.dump()); }
} // namespace PMS::JsonUtils

namespace PMS::DB {
mongocxx::instance m_mongo_instance{};

MongoDBBackend::MongoDBBackend(std::string_view dbhost, std::string_view dbname)
    : Backend(), m_dbhost{dbhost}, m_dbname{dbname}, m_pool{nullptr} {}

ErrorOr<void> MongoDBBackend::Connect() {
  try {
    m_pool = std::make_unique<mongocxx::pool>(mongocxx::uri{fmt::format("mongodb://{}/{}", m_dbhost, m_dbname)});
  } catch (const mongocxx::exception &e) {
    return Error{e.code(), e.what()};
  }
  return outcome::success();
}

ErrorOr<void> MongoDBBackend::Connect(std::string_view user, std::string_view password) {
  try {
    m_pool = std::make_unique<mongocxx::pool>(
        mongocxx::uri{fmt::format("mongodb://{}:{}@{}/{}", user, password, m_dbhost, m_dbname)});
  } catch (const mongocxx::exception &e) {
    return Error{e.code(), e.what()};
  }
  return outcome::success();
}

ErrorOr<void> MongoDBBackend::SetupIfNeeded() {
  using bsoncxx::builder::basic::kvp;
  using bsoncxx::builder::basic::make_document;

  auto poolEntry = m_pool->acquire();
  auto db = (*poolEntry)[m_dbname];

  if (!db.has_collection("jobs")) {
    spdlog::debug("Creating indexes for the \"jobs\" collection");
    mongocxx::options::index index_options{};
    // hash is a unique index
    index_options.unique(true);
    try {
      db["jobs"].create_index(make_document(kvp("hash", 1)), index_options);
    } catch (const mongocxx::exception &e) {
      return Error{e.code(), e.what()};
    }

    // task is not a unique index :)
    index_options.unique(false);
    try {
      db["jobs"].create_index(make_document(kvp("task", 1)), index_options);
    } catch (const mongocxx::exception &e) {
      return Error{e.code(), e.what()};
    }
  }
  if (!db.has_collection("tasks")) {
    spdlog::debug("Creating indexes for the \"tasks\" collection");
    mongocxx::options::index index_options{};
    // task name is a unique index
    index_options.unique(true);
    try {
      db["tasks"].create_index(make_document(kvp("name", 1)), index_options);
    } catch (const mongocxx::exception &e) {
      return Error{e.code(), e.what()};
    }
  }
  if (!db.has_collection("pilots")) {
    spdlog::debug("Creating indexes for the \"pilots\" collection");
    mongocxx::options::index index_options{};
    // pilot uuid is a unique index
    index_options.unique(true);

    try {
      db["pilots"].create_index(make_document(kvp("uuid", 1)), index_options);
    } catch (const mongocxx::exception &e) {
      return Error{e.code(), e.what()};
    }
  }

  return outcome::success();
}

json MongoDBBackend::MatchesToJson(const Queries::Matches &matches) {
  if (matches.empty())
    return "{}"_json;

  json result = json::object();

  for (const auto &[field, value, op] : matches) {
    // let's split the field by dots
    auto sub_keys = std::string_view{field} | std::views::split('.');

    // let's add the json element at the right depth, following the sub_keys path
    auto *current = &result;
    for (const auto key_v : sub_keys) {
      std::string_view key{key_v.begin(), key_v.size()};

      // let's walk down the json hierarchy key by key, creating objects if needed
      if (current->empty()) {
        (*current)[key] = json::object();
      }
      current = &(*current)[key];
    }

    switch (op) {
    case Queries::ComparisonOp::EQ:
      *current = value;
      break;
    default:
      std::string op_name{magic_enum::enum_name(op)};
      std::ranges::transform(op_name, op_name.begin(), ::tolower);
      *current = json{{fmt::format("${}", op_name), value}};
      break;
    }
  }

  return result;
}

json MongoDBBackend::UpdatesToJson(const Queries::Updates &updates) {
  if (updates.empty())
    return {};

  json result = json::object();

  for (const auto &[field, value, op] : updates) {
    std::string op_name{magic_enum::enum_name(op)};
    std::ranges::transform(op_name, op_name.begin(), ::tolower);

    op_name.insert(0, "$");
    result[op_name][field] = value;
  }

  return result;
}

ErrorOr<mongocxx::model::write> MongoDBBackend::QueryToWriteOp(const Queries::Query &query) {
  return std::visit(PMS::Utils::overloaded{
                        [&](const Queries::Insert &query) -> ErrorOr<mongocxx::model::write> {
                          if (query.documents.size() > 1) {
                            return Error{std::errc::not_supported, "Insert many not supported by mongocxx driver"};
                          }

                          return mongocxx::model::insert_one{JsonUtils::json2bson(query.documents[0])};
                        },
                        [&](const Queries::Update &query) -> ErrorOr<mongocxx::model::write> {
                          switch (query.options.limit) {
                          case 1:
                            return mongocxx::model::update_one{JsonUtils::json2bson(MatchesToJson(query.match)),
                                                               JsonUtils::json2bson(UpdatesToJson(query.update))};
                          default:
                            return mongocxx::model::update_many{JsonUtils::json2bson(MatchesToJson(query.match)),
                                                                JsonUtils::json2bson(UpdatesToJson(query.update))};
                          }
                        },
                        [&](const Queries::Delete &query) -> ErrorOr<mongocxx::model::write> {
                          switch (query.options.limit) {
                          case 1:
                            return mongocxx::model::delete_one{JsonUtils::json2bson(MatchesToJson(query.match))};
                          default:
                            return mongocxx::model::delete_many{JsonUtils::json2bson(MatchesToJson(query.match))};
                          }
                        },
                        [](auto &query) -> ErrorOr<mongocxx::model::write> {
                          return Error{std::errc::not_supported,
                                       fmt::format("Query type {} not supported in bulk writes", query.name)};
                        },
                    },
                    query);
}

ErrorOr<QueryResult> MongoDBBackend::RunQuery(Queries::Query query) {
  auto poolEntry = m_pool->acquire();
  auto db = (*poolEntry)[m_dbname];

  return std::visit(
      PMS::Utils::overloaded{
          // ------------ Find queries ------------
          [&](Queries::Find &query) -> ErrorOr<QueryResult> {
            mongocxx::options::find options;
            options.limit(query.options.limit);
            options.skip(query.options.skip);
            if (!query.filter.empty())
              options.projection(JsonUtils::json2bson(query.filter));

            QueryResult result;
            try {
              auto query_result = db[query.collection].find(JsonUtils::json2bson(MatchesToJson(query.match)), options);
              std::transform(query_result.begin(), query_result.end(), std::back_inserter(result),
                             [](const auto &doc) { return JsonUtils::bson2json(doc); });
            } catch (const mongocxx::exception &e) {
              return Error{e.code(), e.what()};
            }
            return result;
          },
          // ------------ FindOneAndUpdate queries ------------
          [&](Queries::FindOneAndUpdate &query) -> ErrorOr<QueryResult> {
            mongocxx::options::find_one_and_update options;
            options.bypass_document_validation(query.options.bypass_document_validation);
            if (!query.filter.empty())
              options.projection(JsonUtils::json2bson(query.filter));

            QueryResult result;
            try {
              auto query_result =
                  db[query.collection].find_one_and_update(JsonUtils::json2bson(MatchesToJson(query.match)),
                                                           JsonUtils::json2bson(UpdatesToJson(query.update)), options);
              if (query_result)
                result = JsonUtils::bson2json(query_result.value());
            } catch (const mongocxx::exception &e) {
              return Error{e.code(), e.what()};
            }
            return result;
          },
          // ------------ Insert queries ------------
          [&](const Queries::Insert &query) -> ErrorOr<QueryResult> {
            mongocxx::options::insert options;
            options.bypass_document_validation(query.options.bypass_document_validation);
            QueryResult result;

            try {
              switch (query.documents.size()) {
              case 1: {
                // If we're inserting only one document we use insert_one
                auto query_result = db[query.collection].insert_one(JsonUtils::json2bson(query.documents[0]));
                if (!query_result) {
                  return Error{std::errc::operation_canceled, "Insert not acknowledged"};
                }
                result["inserted_id"] = query_result.value().inserted_id().get_oid().value.to_string();
              } break;
              default: {
                // If we're inserting more than one document we use insert_many
                std::vector<bsoncxx::document::view_or_value> to_be_inserted;
                std::ranges::transform(query.documents, std::back_inserter(to_be_inserted),
                                       [&](const auto &doc) { return JsonUtils::json2bson(doc); });
                auto query_result = db[query.collection].insert_many(to_be_inserted);
                if (!query_result) {
                  return Error{std::errc::operation_canceled, "Insert not acknowledged"};
                }
                result["inserted_count"] = query_result.value().inserted_count();
              } break;
              }
            } catch (const mongocxx::exception &e) {
              return Error{e.code(), e.what()};
            }

            return result;
          },
          // ------------ Update queries ------------
          [&](Queries::Update &query) -> ErrorOr<QueryResult> {
            mongocxx::options::update options;
            options.upsert(query.options.upsert);
            QueryResult result;

            try {
              switch (query.options.limit) {
              case 1: {
                // If we're updating only one document we use update_one
                auto query_result =
                    db[query.collection].update_one(JsonUtils::json2bson(MatchesToJson(query.match)),
                                                    JsonUtils::json2bson(UpdatesToJson(query.update)), options);
                if (!query_result) {
                  return Error{std::errc::operation_canceled, "Update not acknowledged"};
                }
                result["matched_count"] = query_result.value().matched_count();
                result["modified_count"] = query_result.value().modified_count();
              } break;
              default: {
                // If we're updating more than one document we use update_many
                auto query_result =
                    db[query.collection].update_many(JsonUtils::json2bson(MatchesToJson(query.match)),
                                                     JsonUtils::json2bson(UpdatesToJson(query.update)), options);
                if (!query_result) {
                  return Error{std::errc::operation_canceled, "Update not acknowledged"};
                }
                result["matched_count"] = query_result.value().matched_count();
                result["modified_count"] = query_result.value().modified_count();
              } break;
              }
            } catch (const mongocxx::exception &e) {
              return Error{e.code(), e.what()};
            }

            return result;
          },
          // ------------ Delete queries ------------
          [&](Queries::Delete &query) -> ErrorOr<QueryResult> {
            QueryResult result;

            try {
              switch (query.options.limit) {
              case 1: {
                // If we're deleting only one document we use delete_one
                auto query_result = db[query.collection].delete_one(JsonUtils::json2bson(MatchesToJson(query.match)));
                if (!query_result) {
                  return Error{std::errc::operation_canceled, "Delete not acknowledged"};
                }
                result["deleted_count"] = query_result.value().deleted_count();
              } break;
              default: {
                // If we're deleting more than one document we use delete_many
                auto query_result = db[query.collection].delete_many(JsonUtils::json2bson(MatchesToJson(query.match)));
                if (!query_result) {
                  return Error{std::errc::operation_canceled, "Delete not acknowledged"};
                }
                result["deleted_count"] = query_result.value().deleted_count();
              } break;
              }
            } catch (const mongocxx::exception &e) {
              return Error{e.code(), e.what()};
            }

            return result;
          },
          [&](const Queries::Count &query) -> ErrorOr<QueryResult> {
            try {
              auto result = db[query.collection].count_documents(JsonUtils::json2bson(MatchesToJson(query.match)));
              return json{{"count", result}};
            } catch (const mongocxx::exception &e) {
              return Error{e.code(), e.what()};
            }
          },
          [&](const Queries::Distinct &query) -> ErrorOr<QueryResult> {
            QueryResult result;

            try {
              auto query_result =
                  db[query.collection].distinct(query.field, JsonUtils::json2bson(MatchesToJson(query.match)));

              std::vector<json> dummy;

              std::transform(query_result.begin(), query_result.end(), std::back_inserter(dummy),
                             [](const auto &doc) { return JsonUtils::bson2json(doc); });

              if (dummy.empty()) {
                return Error{std::errc::no_such_file_or_directory, "No distinct values found"};
              }

              if (dummy.size() > 1) {
                return Error{std::errc::not_supported, "Distinct query returned more than one value. Rporto this to "
                                                       "the developers which should check if the mongocxx API changed"};
              }

              if (dummy[0]["ok"] == 0) {
                return Error{std::errc::no_such_file_or_directory,
                             fmt::format("Unexpected result to Distinct query: {}", dummy[0].dump())};
              }

              result = dummy[0]["values"];
              return result;
            } catch (const mongocxx::exception &e) {
              return Error{e.code(), e.what()};
            } catch (const std::exception &e) {
              return Error{std::errc::invalid_argument, e.what()};
            }

            return result;
          },
      },
      query);
}

ErrorOr<QueryResult> MongoDBBackend::BulkWrite(std::string_view collection, std::vector<Queries::Query> queries) {
  std::vector<mongocxx::model::write> write_ops;

  auto wops = queries | std::views::transform(QueryToWriteOp) |
              std::views::filter([](auto &&op) { return op.has_value(); }) |
              std::views::transform([](auto &&op) { return std::move(op.value()); });

  std::ranges::copy(wops, std::back_inserter(write_ops));

  auto poolEntry = m_pool->acquire();
  auto db = (*poolEntry)[m_dbname];

  try {
    auto query_result = db[collection].bulk_write(write_ops);
    if (!query_result) {
      return Error{std::errc::operation_canceled, "BulkWrite not acknowledged"};
    }

    QueryResult result;
    result["inserted_count"] = query_result.value().inserted_count();
    result["matched_count"] = query_result.value().matched_count();
    result["modified_count"] = query_result.value().modified_count();
    result["deleted_count"] = query_result.value().deleted_count();

    return result;
  } catch (const mongocxx::exception &e) {
    return Error{e.code(), e.what()};
  }
}
} // namespace PMS::DB
