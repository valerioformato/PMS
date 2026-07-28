#include <array>
#include <chrono>
#include <cstdlib>
#include <future>
#include <memory>
#include <ranges>
#include <string>

#include <catch2/catch_test_macros.hpp>
#include <catch2/trompeloeil.hpp>

#include "db/backends/MongoDB/MongoDBBackend.h"

using json = nlohmann::json;
using namespace PMS::DB;

namespace PMS::Tests::MongoDBBackend {

SCENARIO("MatchesToJson function converts match objects to JSON format", "[MatchesToJson]") {
  GIVEN("A match object with simple fields") {
    Queries::Matches match = {{"field1", "value1"}, {"field2", "value2"}};

    WHEN("MatchesToJson is called") {
      json result = ::MongoDBBackend::MatchesToJson(match);

      THEN("The result should be a JSON object with the same fields and values") {
        json expected = {{"field1", "value1"}, {"field2", "value2"}};

        REQUIRE(result == expected);
      }
    }
  }

  GIVEN("A match object with nested fields") {
    Queries::Matches match = {{"field1", "value1"}, {"nested.field2", "value2"}};

    WHEN("MatchesToJson is called") {
      json result = ::MongoDBBackend::MatchesToJson(match);

      THEN("The result should be a JSON object with nested fields") {
        json expected = {{"field1", "value1"}, {"nested", {{"field2", "value2"}}}};

        REQUIRE(result == expected);
      }
    }
  }

  GIVEN("A match object with multiple nested fields") {
    Queries::Matches match = {
        {"field1", "value1"}, {"nested.field2", "value2"}, {"nested.deeply_nested.field3", "value3"}};

    WHEN("MatchesToJson is called") {
      json result = ::MongoDBBackend::MatchesToJson(match);

      THEN("The result should be a JSON object with deeply nested fields") {
        json expected = {{"field1", "value1"},
                         {"nested", {{"field2", "value2"}, {"deeply_nested", {{"field3", "value3"}}}}}};

        REQUIRE(result == expected);
      }
    }
  }

  GIVEN("An empty match object") {
    Queries::Matches match = {};

    WHEN("MatchesToJson is called") {
      json result = ::MongoDBBackend::MatchesToJson(match);

      THEN("The result should be an empty JSON object") {
        json expected = "{}"_json;

        REQUIRE(result == expected);
      }
    }
  }
  GIVEN("A set of matches") {
    WHEN("Matches is empty") {
      Queries::Matches matches;
      json result = ::MongoDBBackend::MatchesToJson(matches);

      THEN("The result should be an empty JSON object") { REQUIRE(result.empty()); }
    }

    WHEN("Matches contains a single match with EQ operation") {
      Queries::Matches matches = {{"field1", "value1", Queries::ComparisonOp::EQ}};
      json result = ::MongoDBBackend::MatchesToJson(matches);

      THEN("The result should contain the field with the correct value") { REQUIRE(result["field1"] == "value1"); }
    }

    WHEN("Matches contains multiple matches with different operations") {
      Queries::Matches matches = {{"field1", "value1", Queries::ComparisonOp::EQ},
                                  {"field2", 10, Queries::ComparisonOp::GT},
                                  {"field3", 5, Queries::ComparisonOp::LT}};
      json result = ::MongoDBBackend::MatchesToJson(matches);

      THEN("The result should contain all fields with the correct values and operations") {
        REQUIRE(result["field1"] == "value1");

        REQUIRE(result["field2"].is_object());
        REQUIRE(result["field2"]["$gt"] == 10);

        REQUIRE(result["field3"].is_object());
        REQUIRE(result["field3"]["$lt"] == 5);
      }
    }

    WHEN("Matches contains nested fields") {
      Queries::Matches matches = {{"field1.subfield1", "value1", Queries::ComparisonOp::EQ},
                                  {"field2.subfield2", 10, Queries::ComparisonOp::GT}};
      json result = ::MongoDBBackend::MatchesToJson(matches);

      THEN("The result should contain nested JSON objects with the correct values and operations") {
        REQUIRE(result["field1"]["subfield1"] == "value1");

        REQUIRE(result["field2"]["subfield2"].is_object());
        REQUIRE(result["field2"]["subfield2"]["$gt"] == 10);
      }
    }
  }
}
SCENARIO("MongoDBBackend::UpdatesToJson", "[MongoDBBackend]") {
  GIVEN("An empty updates list") {
    Queries::Updates updates;

    WHEN("UpdatesToJson is called") {
      auto result = ::MongoDBBackend::UpdatesToJson(updates);

      THEN("The result should be an empty JSON object") { REQUIRE(result.empty()); }
    }
  }

  GIVEN("A single update with SET operation") {
    Queries::Updates updates = {{"field1", "value1", Queries::UpdateOp::SET}};

    WHEN("UpdatesToJson is called") {
      auto result = ::MongoDBBackend::UpdatesToJson(updates);

      THEN("The result should contain the correct update operation") {
        json expected = {{"$set", {{"field1", "value1"}}}};
        REQUIRE(result == expected);
      }
    }
  }

  GIVEN("Multiple updates with different operations") {
    Queries::Updates updates = {
        {"field1", "value1", Queries::UpdateOp::SET},
        {"field2", 42, Queries::UpdateOp::INC},
    };

    WHEN("UpdatesToJson is called") {
      auto result = ::MongoDBBackend::UpdatesToJson(updates);

      THEN("The result should contain all the update operations") {
        json expected = {{"$set", {{"field1", "value1"}}}, {"$inc", {{"field2", 42}}}};
        REQUIRE(result == expected);
      }
    }
  }

  GIVEN("Multiple updates with the same operations on different fields") {
    Queries::Updates updates = {
        {"field1", "value1", Queries::UpdateOp::SET},
        {"field2", 42, Queries::UpdateOp::SET},
    };

    WHEN("UpdatesToJson is called") {
      auto result = ::MongoDBBackend::UpdatesToJson(updates);

      THEN("The result should contain all the update operations") {
        json expected = {{"$set", {{"field1", "value1"}, {"field2", 42}}}};
        REQUIRE(result == expected);
      }
    }
  }

  GIVEN("An update with a nested field") {
    Queries::Updates updates = {{"nested.field", "value", Queries::UpdateOp::SET}};

    WHEN("UpdatesToJson is called") {
      auto result = ::MongoDBBackend::UpdatesToJson(updates);

      THEN("The result should contain the correct nested update operation") {
        json expected = {{"$set", {{"nested.field", "value"}}}};
        REQUIRE(result == expected);
      }
    }
  }

  GIVEN("An update with multiple nested fields") {
    Queries::Updates updates = {{"nested.field1", "value1", Queries::UpdateOp::SET},
                                {"nested.field2", 100, Queries::UpdateOp::INC}};

    WHEN("UpdatesToJson is called") {
      auto result = ::MongoDBBackend::UpdatesToJson(updates);

      THEN("The result should contain the correct nested update operations") {
        json expected = {{"$set", {{"nested.field1", "value1"}}}, {"$inc", {{"nested.field2", 100}}}};
        REQUIRE(result == expected);
      }
    }
  }

  GIVEN("An update with a complex nested field structure") {
    Queries::Updates updates = {{"nested.level1.level2.field", "value", Queries::UpdateOp::SET}};

    WHEN("UpdatesToJson is called") {
      auto result = ::MongoDBBackend::UpdatesToJson(updates);

      THEN("The result should contain the correct complex nested update operation") {
        json expected = {{"$set", {{"nested.level1.level2.field", "value"}}}};
        REQUIRE(result == expected);
      }
    }
  }
} // namespace PMS::Tests::MongoDBBackend

SCENARIO("MongoDBBackend::QueryToWriteOp", "[MongoDBBackend]") {
  GIVEN("A valid Insert query") {
    ::Queries::Insert insertQuery;
    insertQuery.collection = "test_collection";
    insertQuery.documents = {R"({"name": "test_document"})"_json};

    WHEN("QueryToWriteOp is called") {
      auto result = ::MongoDBBackend::QueryToWriteOp(insertQuery);

      THEN("It should return a valid insert_one write operation") {
        REQUIRE(result.has_value());
        auto writeOp = std::move(result.value());
        REQUIRE(writeOp.type() == mongocxx::write_type::k_insert_one);
      }
    }
  }

  GIVEN("A valid Update query with limit 1") {
    ::Queries::Update updateQuery;
    updateQuery.collection = "test_collection";
    updateQuery.match = {{"name", "test_document"}};
    updateQuery.update = {{"$set", {{"name", "updated_document"}}}};
    updateQuery.options.limit = 1;

    WHEN("QueryToWriteOp is called") {
      auto result = ::MongoDBBackend::QueryToWriteOp(updateQuery);

      THEN("It should return a valid update_one write operation") {
        REQUIRE(result.has_value());
        auto writeOp = std::move(result.value());
        REQUIRE(writeOp.type() == mongocxx::write_type::k_update_one);
      }
    }
  }

  GIVEN("A valid Update query with limit > 1") {
    ::Queries::Update updateQuery;
    updateQuery.collection = "test_collection";
    updateQuery.match = {{"name", "test_document"}};
    updateQuery.update = {{"$set", {{"name", "updated_document"}}}};

    WHEN("QueryToWriteOp is called") {
      auto result = ::MongoDBBackend::QueryToWriteOp(updateQuery);

      THEN("It should return a valid update_many write operation") {
        REQUIRE(result.has_value());
        auto writeOp = std::move(result.value());
        REQUIRE(writeOp.type() == mongocxx::write_type::k_update_many);
      }
    }
  }

  GIVEN("A valid Delete query with limit 1") {
    ::Queries::Delete deleteQuery;
    deleteQuery.collection = "test_collection";
    deleteQuery.match = {{"name", "test_document"}};
    deleteQuery.options.limit = 1;

    WHEN("QueryToWriteOp is called") {
      auto result = ::MongoDBBackend::QueryToWriteOp(deleteQuery);

      THEN("It should return a valid delete_one write operation") {
        REQUIRE(result.has_value());
        auto writeOp = std::move(result.value());
        REQUIRE(writeOp.type() == mongocxx::write_type::k_delete_one);
      }
    }
  }

  GIVEN("A valid Delete query with limit > 1") {
    ::Queries::Delete deleteQuery;
    deleteQuery.collection = "test_collection";
    deleteQuery.match = {{"name", "test_document"}};

    WHEN("QueryToWriteOp is called") {
      auto result = ::MongoDBBackend::QueryToWriteOp(deleteQuery);

      THEN("It should return a valid delete_many write operation") {
        REQUIRE(result.has_value());
        auto writeOp = std::move(result.value());
        REQUIRE(writeOp.type() == mongocxx::write_type::k_delete_many);
      }
    }
  }

  GIVEN("An unsupported query type") {
    ::Queries::Count countQuery;
    countQuery.collection = "test_collection";
    countQuery.match = {{"name", "test_document"}};

    WHEN("QueryToWriteOp is called") {
      auto result = ::MongoDBBackend::QueryToWriteOp(countQuery);

      THEN("It should return an error") {
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().Message() == "Query type Count not supported in bulk writes");
      }
    }
  }
}

SCENARIO("MongoDBBackend::FindOneAndUpdate atomically claims one document",
         "[MongoDBBackend][FindOneAndUpdate][integration]") {
  const auto *host = std::getenv("PMS_TEST_MONGODB_HOST");
  if (host == nullptr) {
    SKIP("Set PMS_TEST_MONGODB_HOST to run MongoDB integration tests");
  }

  const auto *configured_db = std::getenv("PMS_TEST_MONGODB_DB");
  const std::string_view database = configured_db == nullptr ? "pms_tests" : configured_db;
  PMS::DB::MongoDBBackend backend{host, database};
  REQUIRE(backend.Connect().has_value());

  const auto suffix = std::chrono::steady_clock::now().time_since_epoch().count();
  const auto hash = fmt::format("find-one-and-update-{}", suffix);

  struct JobCleanup {
    PMS::DB::MongoDBBackend &backend;
    std::string hash;

    ~JobCleanup() {
      static_cast<void>(backend.RunQuery(Queries::Delete{
          .collection = "jobs",
          .options = {.limit = 1},
          .match = {{"hash", hash}},
      }));
    }
  } cleanup{backend, hash};

  const json job = {
      {"hash", hash}, {"task", "task-1"}, {"status", "Pending"}, {"retries", 0}, {"tags", json::array()},
  };
  auto insert_result = backend.RunQuery(Queries::Insert{
      .collection = "jobs",
      .documents = {job},
  });
  REQUIRE(insert_result.has_value());

  const Queries::FindOneAndUpdate claim_query{
      .collection = "jobs",
      .match =
          {
              {"hash", hash},
              {"status", "Pending"},
          },
      .update =
          {
              {"status", "Claimed"},
              {"retries", 1, Queries::UpdateOp::INC},
          },
      .filter = R"({"_id": 0})"_json,
  };

  auto first = std::async(std::launch::async, [&backend, claim_query] { return backend.RunQuery(claim_query); });
  auto second = std::async(std::launch::async, [&backend, claim_query] { return backend.RunQuery(claim_query); });
  const auto results = std::array{first.get(), second.get()};

  REQUIRE(results[0].has_value());
  REQUIRE(results[1].has_value());

  const auto successful_claims =
      std::ranges::count_if(results, [](const auto &result) { return !result.value().empty(); });
  REQUIRE(successful_claims == 1);

  const auto winner = std::ranges::find_if(results, [](const auto &result) { return !result.value().empty(); });
  REQUIRE(winner != results.end());
  REQUIRE(winner->value()["hash"] == hash);
  REQUIRE(winner->value()["status"] == "Pending");
  REQUIRE(winner->value()["retries"] == 0);

  const auto stored_job = backend.RunQuery(Queries::Find{
      .collection = "jobs",
      .options = {.limit = 1},
      .match = {{"hash", hash}},
      .filter = R"({"_id": 0})"_json,
  });
  REQUIRE(stored_job.has_value());
  REQUIRE(stored_job->size() == 1);
  REQUIRE((*stored_job)[0]["status"] == "Claimed");
  REQUIRE((*stored_job)[0]["retries"] == 1);
}
} // namespace PMS::Tests::MongoDBBackend
