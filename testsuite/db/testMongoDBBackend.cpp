#include <memory>

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
      json result = ::PMS::DB::MongoDBBackend::MatchesToJson(match);

      THEN("The result should be a JSON object with the same fields and values") {
        json expected = {{"field1", "value1"}, {"field2", "value2"}};

        REQUIRE(result == expected);
      }
    }
  }

  GIVEN("A match object with nested fields") {
    Queries::Matches match = {{"field1", "value1"}, {"nested.field2", "value2"}};

    WHEN("MatchesToJson is called") {
      json result = ::PMS::DB::MongoDBBackend::MatchesToJson(match);

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
      json result = ::PMS::DB::MongoDBBackend::MatchesToJson(match);

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
      json result = ::PMS::DB::MongoDBBackend::MatchesToJson(match);

      THEN("The result should be an empty JSON object") {
        json expected = "{}"_json;

        REQUIRE(result == expected);
      }
    }
  }
  GIVEN("A set of matches") {
    WHEN("Matches is empty") {
      Queries::Matches matches;
      json result = ::PMS::DB::MongoDBBackend::MatchesToJson(matches);

      THEN("The result should be an empty JSON object") { REQUIRE(result.empty()); }
    }

    WHEN("Matches contains a single match with EQ operation") {
      Queries::Matches matches = {{"field1", "value1", Queries::ComparisonOp::EQ}};
      json result = ::PMS::DB::MongoDBBackend::MatchesToJson(matches);

      THEN("The result should contain the field with the correct value") { REQUIRE(result["field1"] == "value1"); }
    }

    WHEN("Matches contains multiple matches with different operations") {
      Queries::Matches matches = {{"field1", "value1", Queries::ComparisonOp::EQ},
                                  {"field2", 10, Queries::ComparisonOp::GT},
                                  {"field3", 5, Queries::ComparisonOp::LT}};
      json result = ::PMS::DB::MongoDBBackend::MatchesToJson(matches);

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
      json result = ::PMS::DB::MongoDBBackend::MatchesToJson(matches);

      THEN("The result should contain nested JSON objects with the correct values and operations") {
        REQUIRE(result["field1"]["subfield1"] == "value1");

        REQUIRE(result["field2"]["subfield2"].is_object());
        REQUIRE(result["field2"]["subfield2"]["$gt"] == 10);
      }
    }
  }
}
} // namespace PMS::Tests::MongoDBBackend
