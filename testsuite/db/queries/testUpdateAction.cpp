#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <nlohmann/json.hpp>
#include <ranges>

#include "db/queries/Queries.h"
#include "db/queries/Update.h"

#include "TestUtils.h"

using namespace PMS::DB::Queries;
using json = nlohmann::json;

SCENARIO("ToUpdates function converts JSON to Updates correctly", "[ToUpdates]") {
  GIVEN("A valid JSON with update operations") {
    json match_json = R"({
      "$set" : {"field1": "value1"},
      "$inc" : {"field2": 10},
      "$mul" : {"field3": 2},
      "$min" : {"field4": 5},
      "$max" : {"field5": 100}
    })"_json;

    WHEN("ToUpdates is called") {
      auto result = ToUpdates(match_json);

      THEN("It should return a successful outcome") { REQUIRE(result.has_value()); }

      THEN("The Updates vector should contain the correct UpdateActions") {
        auto updates = result.value();
        REQUIRE(updates.size() == 6);

        REQUIRE(::PMS::Tests::Utils::RangeContains(updates, UpdateAction{"field1", "value1", UpdateOp::SET}));
        REQUIRE(::PMS::Tests::Utils::RangeContains(updates, UpdateAction{"field2", 10, UpdateOp::INC}));
        REQUIRE(::PMS::Tests::Utils::RangeContains(updates, UpdateAction{"field3", 2, UpdateOp::MUL}));
        REQUIRE(::PMS::Tests::Utils::RangeContains(updates, UpdateAction{"field4", 5, UpdateOp::MIN}));
        REQUIRE(::PMS::Tests::Utils::RangeContains(updates, UpdateAction{"field5", 100, UpdateOp::MAX}));
      }
    }
  }

  GIVEN("An invalid JSON with unknown update operations") {
    json match_json = R"({
      "$unknown" : {"field1": "value1"}
    })"_json;

    WHEN("ToUpdates is called") {
      auto result = ToUpdates(match_json);

      THEN("It should return a failure outcome") {
        REQUIRE(result.has_error());
        REQUIRE(result.assume_error().Code() == std::errc::invalid_argument);
        REQUIRE(result.assume_error().Message() == "Invalid comparison operator: $unknown");
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
