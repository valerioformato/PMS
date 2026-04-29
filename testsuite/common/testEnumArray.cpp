#include <catch2/catch_test_macros.hpp>
#include <magic_enum/magic_enum.hpp>

#include "common/EnumArray.h"
#include "common/Job.h"

using PMS::JobStatus;

namespace PMS::Tests::Common {

SCENARIO("EnumArray value-initialization zeroes all elements", "[EnumArray]") {
  GIVEN("A value-initialized EnumArray") {
    EnumArray<unsigned int, JobStatus> arr{};
    THEN("All elements are zero") {
      for (auto status : magic_enum::enum_values<JobStatus>()) {
        REQUIRE(arr[status] == 0u);
      }
    }
  }
}

SCENARIO("EnumArray enum-keyed read/write", "[EnumArray]") {
  GIVEN("A value-initialized EnumArray") {
    EnumArray<unsigned int, JobStatus> arr{};

    WHEN("A value is assigned via an enum key") {
      arr[JobStatus::Done] = 42u;

      THEN("Reading back via the same key returns the assigned value") { REQUIRE(arr[JobStatus::Done] == 42u); }

      THEN("Other elements remain zero") {
        REQUIRE(arr[JobStatus::Pending] == 0u);
        REQUIRE(arr[JobStatus::Running] == 0u);
        REQUIRE(arr[JobStatus::Failed] == 0u);
      }
    }
  }
}

SCENARIO("EnumArray accumulates counts across multiple keys", "[EnumArray]") {
  GIVEN("An array with several populated counts") {
    EnumArray<unsigned int, JobStatus> arr{};
    arr[JobStatus::Pending] = 3u;
    arr[JobStatus::Done] = 5u;
    arr[JobStatus::Failed] = 2u;

    THEN("Each key is independently readable") {
      REQUIRE(arr[JobStatus::Pending] == 3u);
      REQUIRE(arr[JobStatus::Done] == 5u);
      REQUIRE(arr[JobStatus::Failed] == 2u);
    }

    THEN("The sum of those entries matches expectations") {
      unsigned int total = arr[JobStatus::Pending] + arr[JobStatus::Done] + arr[JobStatus::Failed];
      REQUIRE(total == 10u);
    }
  }
}

SCENARIO("EnumArray .at() provides bounds-checked access for valid keys", "[EnumArray]") {
  GIVEN("A value-initialized EnumArray") {
    EnumArray<unsigned int, JobStatus> arr{};
    arr[JobStatus::Running] = 7u;

    THEN(".at() returns the same value as operator[] for valid keys") {
      REQUIRE(arr.at(JobStatus::Running) == arr[JobStatus::Running]);
    }
  }
}

SCENARIO("EnumArray copy construction", "[EnumArray]") {
  GIVEN("An array with populated values") {
    EnumArray<unsigned int, JobStatus> original{};
    original[JobStatus::Pending] = 4u;
    original[JobStatus::Done] = 9u;

    WHEN("A copy is made") {
      EnumArray<unsigned int, JobStatus> copy{};
      copy = original;

      THEN("The copy has the same values") {
        REQUIRE(copy[JobStatus::Pending] == 4u);
        REQUIRE(copy[JobStatus::Done] == 9u);
      }

      THEN("Modifying the copy does not affect the original") {
        copy[JobStatus::Pending] = 0u;
        REQUIRE(original[JobStatus::Pending] == 4u);
      }
    }
  }
}

} // namespace PMS::Tests::Common
