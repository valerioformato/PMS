#include <db/harness/Harness.h>
#include <memory>

#include <catch2/catch_test_macros.hpp>
#include <catch2/trompeloeil.hpp>

namespace PMS::Tests::Harness {
class MockBackend : public trompeloeil::mock_interface<PMS::DB::Backend> {
public:
  IMPLEMENT_MOCK0(Connect);
};

TEST_CASE("Harness Connect Test", "[Harness]") {
  trompeloeil::stream_tracer tracer{std::cout};

  auto mockBackend = std::make_unique<MockBackend>();
  auto mockBackendPtr = mockBackend.get();

  PMS::DB::Harness harness(std::move(mockBackend));

  REQUIRE_CALL(*mockBackendPtr, Connect()).RETURN(outcome::success());

  auto connection_result = harness.Connect();
  REQUIRE(connection_result.has_value());
}

TEST_CASE("Harness RunQuery Test", "[Harness]") {}

} // namespace PMS::Tests::Harness
