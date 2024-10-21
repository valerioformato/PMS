#include <memory>

#include <catch2/catch_test_macros.hpp>
#include <catch2/trompeloeil.hpp>

#include "db/backends/MongoDB/MongoDBBackend.h"

using json = nlohmann::json;
using namespace PMS::DB;

namespace PMS::Tests::MongoDBBackend {

SCENARIO("ApplyCustomComparisons function modifies JSON object based on overridden comparisons",
         "[ApplyCustomComparisons]") {
  GIVEN("An initial match JSON object and overridden comparisons") {
    json match = {
        {"field1", "value1"},
        {"field2", "value2"},
        {"nested", {{"field3", "value3"}}},
    };

    Queries::OverriddenComparisons comps = {{"field1", Queries::ComparisonOp::GT},
                                            {"nested.field3", Queries::ComparisonOp::LT}};

    WHEN("ApplyCustomComparisons is called") {
      ErrorOr<json> result = ::PMS::DB::MongoDBBackend::ApplyCustomComparisons(match, comps);

      THEN("The result should have the custom comparisons applied") {
        json expected = {
            {"field1", {{"$gt", "value1"}}}, {"field2", "value2"}, {"nested", {{"field3", {{"$lt", "value3"}}}}}};

        REQUIRE(result.has_value());
        REQUIRE(result.assume_value() == expected);
      }
    }
  }

  GIVEN("An initial match JSON object with nested fields and overridden comparisons") {
    json match = {{"field1", "value1"}, {"nested", {{"field2", "value2"}, {"deeply_nested", {{"field3", "value3"}}}}}};

    Queries::OverriddenComparisons comps = {{"nested.deeply_nested.field3", Queries::ComparisonOp::NE}};

    WHEN("ApplyCustomComparisons is called") {
      ErrorOr<json> result = ::PMS::DB::MongoDBBackend::ApplyCustomComparisons(match, comps);

      THEN("The result should have the custom comparisons applied to nested fields") {
        json expected = {{"field1", "value1"},
                         {"nested", {{"field2", "value2"}, {"deeply_nested", {{"field3", {{"$ne", "value3"}}}}}}}};

        REQUIRE(result.has_value());
        REQUIRE(result.assume_value() == expected);
      }
    }
  }

  GIVEN("An initial match JSON object with no matching fields for overridden comparisons") {
    json match = {{"field1", "value1"}, {"field2", "value2"}};

    Queries::OverriddenComparisons comps = {{"nonexistent_field", Queries::ComparisonOp::GT}};

    WHEN("ApplyCustomComparisons is called") {
      ErrorOr<json> result = ::PMS::DB::MongoDBBackend::ApplyCustomComparisons(match, comps);

      THEN("The result should remain unchanged") {
        json expected = {{"field1", "value1"}, {"field2", "value2"}};

        REQUIRE(result.has_error());
        REQUIRE(result.assume_error() == boost::system::errc::invalid_argument);
      }
    }
  }
}
} // namespace PMS::Tests::MongoDBBackend
