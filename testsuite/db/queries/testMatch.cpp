#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <nlohmann/json.hpp>

#include "db/queries/Match.h"

using json = nlohmann::json;
using namespace PMS::DB::Queries;

SCENARIO("ToMatches function", "[ToMatches]") {
  GIVEN("A valid JSON object with different comparison operations") {
    json match_json = R"({
      "key1": {"$eq": "value1"},
      "key2": {"$ne": "value2"},
      "key3": {"$gt": 10},
      "key4": {"$gte": 20},
      "key5": {"$lt": 30},
      "key6": {"$lte": 40},
      "key7": {"$in": ["value3", "value4"]},
      "key8": "value5"
    })"_json;

    WHEN("ToMatches is called") {
      auto result = ToMatches(match_json);

      THEN("It should return a success outcome with the correct Matches") {
        REQUIRE(result.has_value());
        auto matches = result.value();
        REQUIRE(matches.size() == 8);

        CHECK(matches[0] == Match{"key1", "value1", ComparisonOp::EQ});
        CHECK(matches[1] == Match{"key2", "value2", ComparisonOp::NE});
        CHECK(matches[2] == Match{"key3", 10, ComparisonOp::GT});
        CHECK(matches[3] == Match{"key4", 20, ComparisonOp::GTE});
        CHECK(matches[4] == Match{"key5", 30, ComparisonOp::LT});
        CHECK(matches[5] == Match{"key6", 40, ComparisonOp::LTE});
        CHECK(matches[6] == Match{"key7", json::array({"value3", "value4"}), ComparisonOp::IN});
        CHECK(matches[7] == Match{"key8", "value5", ComparisonOp::EQ});
      }
    }
  }

  GIVEN("A JSON object with an invalid comparison operation") {
    json match_json = R"({
      "key1": {"$invalid": "value1"}
    })"_json;

    WHEN("ToMatches is called") {
      auto result = ToMatches(match_json);

      THEN("It should return a failure outcome") {
        REQUIRE(result.has_error());
        REQUIRE(result.assume_error().Code() == std::errc::invalid_argument);
        REQUIRE(result.assume_error().Message() == "Invalid comparison operator: $invalid");
      }
    }
  }

  GIVEN("An empty JSON object") {
    json match_json = "{}"_json;

    WHEN("ToMatches is called") {
      auto result = ToMatches(match_json);

      THEN("It should return a success outcome with an empty Matches vector") {
        REQUIRE(result.has_value());
        auto matches = result.value();
        REQUIRE(matches.empty());
      }
    }
  }
}
