#include <memory>

#include <catch2/catch_test_macros.hpp>
#include <catch2/trompeloeil.hpp>

#include "db/harness/Harness.h"

namespace PMS::Tests::Harness {
class MockBackend : public trompeloeil::mock_interface<PMS::DB::Backend> {
public:
  MAKE_MOCK0(Connect, auto(void)->ErrorOr<void>, override);
  MAKE_MOCK2(Connect, auto(std::string_view, std::string_view)->ErrorOr<void>, override);

  IMPLEMENT_MOCK0(SetupIfNeeded);

  IMPLEMENT_MOCK1(RunQuery);
  IMPLEMENT_MOCK2(BulkWrite);
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
      REQUIRE_CALL(*mockBackendPtr, Connect()).RETURN(Error(std::errc::connection_refused, "Connection refused"));
      auto connection_result = harness.Connect();

      THEN("The connection should fail") { REQUIRE(connection_result.has_error()); }
    }

    WHEN("RunQuery is called") {
      PMS::DB::Queries::Query query = PMS::DB::Queries::Find{};
      REQUIRE_CALL(*mockBackendPtr, RunQuery(query)).RETURN(json{});
      auto query_result = harness.RunQuery(query);

      THEN("The query should be successful") { REQUIRE(query_result.has_value()); }
    }

    WHEN("The backend fails to run the query") {
      PMS::DB::Queries::Query query = PMS::DB::Queries::Find{};
      REQUIRE_CALL(*mockBackendPtr, RunQuery(query)).RETURN(Error(std::errc::operation_canceled, "Operation canceled"));
      auto query_result = harness.RunQuery(query);

      THEN("The query should fail") { REQUIRE(query_result.has_error()); }
    }

    WHEN("BulkWrite is called") {
      std::vector<PMS::DB::Queries::Query> queries;
      REQUIRE_CALL(*mockBackendPtr, BulkWrite("collection", queries)).RETURN(json{});
      auto bulk_write_result = harness.BulkWrite("collection", queries);

      THEN("The bulk write should be successful") { REQUIRE(bulk_write_result.has_value()); }
    }

    WHEN("The backend fails to bulk write") {
      std::vector<PMS::DB::Queries::Query> queries;
      REQUIRE_CALL(*mockBackendPtr, BulkWrite("collection", queries))
          .RETURN(Error(std::errc::operation_canceled, "Operation canceled"));
      auto bulk_write_result = harness.BulkWrite("collection", queries);

      THEN("The bulk write should fail") { REQUIRE(bulk_write_result.has_error()); }
    }

    WHEN("SetupIfNeeded is called") {
      REQUIRE_CALL(*mockBackendPtr, SetupIfNeeded()).RETURN(outcome::success());
      auto setup_result = harness.SetupIfNeeded();

      THEN("The setup should be successful") { REQUIRE(setup_result.has_value()); }
    }

    WHEN("The backend fails to setup") {
      REQUIRE_CALL(*mockBackendPtr, SetupIfNeeded()).RETURN(Error(std::errc::operation_canceled, "Operation canceled"));
      auto setup_result = harness.SetupIfNeeded();

      THEN("The setup should fail") { REQUIRE(setup_result.has_error()); }
    }
  }
}
} // namespace PMS::Tests::Harness
