#include <ranges>

#include <spdlog/spdlog.h>

#include <mongocxx/client.hpp>
#include <mongocxx/exception/exception.hpp>

#include "db/backends/MongoDB/MongoDBBackend.h"

namespace PMS::DB {
ErrorOr<void> MongoDBBackend::Connect() {
  try {
    m_pool = std::make_unique<mongocxx::pool>(mongocxx::uri{fmt::format("mongodb://{}/{}", m_dbhost, m_dbname)});
  } catch (const mongocxx::exception &e) {
    return outcome::failure(e.code());
  }
  return outcome::success();
}

ErrorOr<void> MongoDBBackend::Connect(std::string_view user, std::string_view password) {
  try {
    m_pool = std::make_unique<mongocxx::pool>(
        mongocxx::uri{fmt::format("mongodb://{}:{}@{}/{}", user, password, m_dbhost, m_dbname)});
  } catch (const mongocxx::exception &e) {
    return outcome::failure(e.code());
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
      return outcome::failure(e.code());
    }

    // task is not a unique index :)
    index_options.unique(false);
    try {
      db["jobs"].create_index(make_document(kvp("task", 1)), index_options);
    } catch (const mongocxx::exception &e) {
      return outcome::failure(e.code());
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
      return outcome::failure(e.code());
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
      return outcome::failure(e.code());
    }
  }

  return outcome::success();
}

ErrorOr<QueryResult> MongoDBBackend::RunQuery(Queries::Query query) {
  auto poolEntry = m_pool->acquire();
  auto db = (*poolEntry)[m_dbname];

  return std::visit(PMS::Utils::overloaded{
                        // ------------ Find queries ------------
                        [&](const Queries::Find &query) -> ErrorOr<QueryResult> {
                          mongocxx::options::find options;
                          options.limit(query.options.limit);
                          options.skip(query.options.skip);
                          if (!query.filter.empty())
                            options.projection(JsonUtils::json2bson(query.filter));

                          QueryResult result;
                          try {
                            auto query_result = db[query.collection].find(JsonUtils::json2bson(query.match), options);
                            std::transform(query_result.begin(), query_result.end(), std::back_inserter(result),
                                           [](const auto &doc) { return JsonUtils::bson2json(doc); });
                          } catch (const mongocxx::exception &e) {
                            return ErrorOr<QueryResult>{outcome::failure(e.code())};
                          }
                          return ErrorOr<QueryResult>{result};
                        },
                        // ------------ Insert queries ------------
                        [&](const Queries::Insert &query) {
                          mongocxx::options::insert options;
                          options.bypass_document_validation(query.options.bypass_document_validation);
                          QueryResult result;

                          try {
                            switch (query.documents.size()) {
                            case 1: {
                              // If we're inserting only one document we use insert_one
                              auto query_result =
                                  db[query.collection].insert_one(JsonUtils::json2bson(query.documents[0]));
                              if (!query_result) {
                                return ErrorOr<QueryResult>{outcome::failure(boost::system::errc::invalid_argument)};
                              }
                              result["inserted_id"] = query_result.value().inserted_id().get_string();
                            } break;
                            default: {
                              // If we're inserting more than one document we use insert_many
                              std::vector<bsoncxx::document::view_or_value> to_be_inserted;
                              std::ranges::transform(query.documents, std::back_inserter(to_be_inserted),
                                                     [&](const auto &doc) { return JsonUtils::json2bson(doc); });
                              auto query_result = db[query.collection].insert_many(to_be_inserted);
                              if (!query_result) {
                                return ErrorOr<QueryResult>{outcome::failure(boost::system::errc::invalid_argument)};
                              }
                              result["inserted_count"] = query_result.value().inserted_count();
                            } break;
                            }
                          } catch (const mongocxx::exception &e) {
                            return ErrorOr<QueryResult>{outcome::failure(e.code())};
                          }

                          return ErrorOr<QueryResult>{result};
                        },
                        // ------------ Update queries ------------
                        [&](const Queries::Update &query) {
                          mongocxx::options::update options;
                          options.upsert(query.options.upsert);
                          QueryResult result;

                          try {
                            switch (query.options.limit) {
                            case 1: {
                              // If we're updating only one document we use update_one
                              auto query_result = db[query.collection].update_one(
                                  JsonUtils::json2bson(query.match), JsonUtils::json2bson(query.update), options);
                              if (!query_result) {
                                return ErrorOr<QueryResult>{outcome::failure(boost::system::errc::invalid_argument)};
                              }
                              result["matched_count"] = query_result.value().matched_count();
                              result["modified_count"] = query_result.value().modified_count();
                            } break;
                            default: {
                              // If we're updating more than one document we use update_many
                              auto query_result = db[query.collection].update_many(
                                  JsonUtils::json2bson(query.match), JsonUtils::json2bson(query.update), options);
                              if (!query_result) {
                                return ErrorOr<QueryResult>{outcome::failure(boost::system::errc::invalid_argument)};
                              }
                              result["matched_count"] = query_result.value().matched_count();
                              result["modified_count"] = query_result.value().modified_count();
                            } break;
                            }
                          } catch (const mongocxx::exception &e) {
                            return ErrorOr<QueryResult>{outcome::failure(e.code())};
                          }

                          return ErrorOr<QueryResult>{result};
                        },
                        [&](const Queries::Delete &query) {
                          QueryResult result;

                          try {
                            switch (query.options.limit) {
                            case 1: {
                              // If we're deleting only one document we use delete_one
                              auto query_result = db[query.collection].delete_one(JsonUtils::json2bson(query.match));
                              if (!query_result) {
                                return ErrorOr<QueryResult>{outcome::failure(boost::system::errc::invalid_argument)};
                              }
                              result["deleted_count"] = query_result.value().deleted_count();
                            } break;
                            default: {
                              // If we're deleting more than one document we use delete_many
                              auto query_result = db[query.collection].delete_many(JsonUtils::json2bson(query.match));
                              if (!query_result) {
                                return ErrorOr<QueryResult>{outcome::failure(boost::system::errc::invalid_argument)};
                              }
                              result["deleted_count"] = query_result.value().deleted_count();
                            } break;
                            }
                          } catch (const mongocxx::exception &e) {
                            return ErrorOr<QueryResult>{outcome::failure(e.code())};
                          }

                          return ErrorOr<QueryResult>{result};
                        },
                        [&](const Queries::Count &query) {
                          // TODO: Implement count queries
                          return ErrorOr<QueryResult>{outcome::failure(boost::system::errc::not_supported)};
                        },
                        [&](const Queries::Aggregate &query) {
                          // TODO: Implement aggregate queries
                          return ErrorOr<QueryResult>{outcome::failure(boost::system::errc::not_supported)};
                        },
                    },
                    query);
}

} // namespace PMS::DB
