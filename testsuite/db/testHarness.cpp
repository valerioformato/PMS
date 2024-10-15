#include <memory>

#include <catch2/catch_test_macros.hpp>
#include <catch2/trompeloeil.hpp>

#include "db/harness/Harness.h"

namespace PMS::Tests::Harness {
class MockBackend : public trompeloeil::mock_interface<PMS::DB::Backend> {
public:
  IMPLEMENT_MOCK0(Connect);

  IMPLEMENT_MOCK1(RunQuery);
};

SCENARIO("Harness Test", "[Harness]") {
  GIVEN("A Harness object with a backend") {
    auto mockBackend = std::make_unique<MockBackend>();
    auto mockBackendPtr = mockBackend.get();

    PMS::DB::Harness harness(std::move(mockBackend));

    WHEN("Connect is called") {
      REQUIRE_CALL(*mockBackendPtr, Connect()).RETURN(outcome::success());
      auto connection_result = harness.Connect();

      THEN("The connection should be successful") { REQUIRE(connection_result.has_value()); }
    }

    WHEN("The backend fails to connect") {
      REQUIRE_CALL(*mockBackendPtr, Connect()).RETURN(outcome::failure(boost::system::error_code{}));
      auto connection_result = harness.Connect();

      THEN("The connection should fail") { REQUIRE(connection_result.has_error()); }
    }

    WHEN("RunQuery is called") {
      PMS::DB::Queries::Query query = PMS::DB::Queries::FindOne{};
      REQUIRE_CALL(*mockBackendPtr, RunQuery(query)).RETURN(outcome::success(json{}));
      auto query_result = harness.RunQuery(query);

      THEN("The query should be successful") { REQUIRE(query_result.has_value()); }
    }

    WHEN("The backend fails to run the query") {
      PMS::DB::Queries::Query query = PMS::DB::Queries::FindOne{};
      REQUIRE_CALL(*mockBackendPtr, RunQuery(query)).RETURN(outcome::failure(boost::system::error_code{}));
      auto query_result = harness.RunQuery(query);

      THEN("The query should fail") { REQUIRE(query_result.has_error()); }
    }
  }
}
} // namespace PMS::Tests::Harness