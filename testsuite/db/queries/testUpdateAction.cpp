#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <nlohmann/json.hpp>

#include "db/queries/Update.h"

using namespace PMS::DB::Queries;
using json = nlohmann::json;

SCENARIO("ToUpdates function converts JSON to Updates correctly", "[ToUpdates]") {
  GIVEN("A valid JSON with update operations") {
    json match_json = R"({
      "field1": {"$set": "value1"},
      "field2": {"$inc": 10},
      "field3": {"$mul": 2},
      "field4": {"$min": 5},
      "field5": {"$max": 100},
      "field6": {"$current_date": true}
    })"_json;

    WHEN("ToUpdates is called") {
      auto result = ToUpdates(match_json);

      THEN("It should return a successful outcome") { REQUIRE(result.has_value()); }

      THEN("The Updates vector should contain the correct UpdateActions") {
        auto updates = result.value();
        REQUIRE(updates.size() == 6);

        REQUIRE(updates[0] == UpdateAction{"field1", "value1", UpdateOp::SET});
        REQUIRE(updates[1] == UpdateAction{"field2", 10, UpdateOp::INC});
        REQUIRE(updates[2] == UpdateAction{"field3", 2, UpdateOp::MUL});
        REQUIRE(updates[3] == UpdateAction{"field4", 5, UpdateOp::MIN});
        REQUIRE(updates[4] == UpdateAction{"field5", 100, UpdateOp::MAX});
        REQUIRE(updates[5] == UpdateAction{"field6", true, UpdateOp::CURRENT_DATE});
      }
    }
  }

  GIVEN("An invalid JSON with unknown update operations") {
    json match_json = R"({
      "field1": {"$unknown": "value1"}
    })"_json;

    WHEN("ToUpdates is called") {
      auto result = ToUpdates(match_json);

      THEN("It should return a failure outcome") {
        REQUIRE(result.has_error());
        REQUIRE(result.assume_error() == boost::system::errc::invalid_argument);
      }
    }
  }

  GIVEN("An empty JSON") {
    json match_json = "{}"_json;

    WHEN("ToUpdates is called") {
      auto result = ToUpdates(match_json);

      THEN("It should return a successful outcome with an empty Updates vector") {
        REQUIRE(result.has_value());
        REQUIRE(result.value().empty());
      }
    }
  }
}