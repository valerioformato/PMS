#include <memory>

#include <catch2/catch_test_macros.hpp>
#include <catch2/trompeloeil.hpp>

#include "common/Utils.h"

namespace PMS::Tests::Utils {
SCENARIO("Utils::TokenizeStringView splits string correctly", "[Utils]") {
  using namespace PMS::Utils;

  GIVEN("A string with delimiters") {
    std::string_view input = "this,is,a,test";
    char delimiter = ',';
    std::vector<std::string_view> expected = {"this", "is", "a", "test"};

    WHEN("TokenizeStringView is called") {
      auto result = TokenizeString(input, delimiter);

      THEN("It should split the string correctly") {
        REQUIRE(result.size() == expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
          REQUIRE(result[i] == expected[i]);
        }
      }
    }
  }

  GIVEN("An empty string") {
    std::string_view input = "";
    char delimiter = ',';
    std::vector<std::string_view> expected = {""};

    WHEN("TokenizeString is called") {
      auto result = TokenizeString(input, delimiter);

      THEN("It should return a vector with one empty string") {
        REQUIRE(result.size() == expected.size());
        REQUIRE(result[0] == expected[0]);
      }
    }
  }

  GIVEN("A string with no delimiters") {
    std::string_view input = "test";
    char delimiter = ',';
    std::vector<std::string_view> expected = {"test"};

    WHEN("TokenizeString is called") {
      auto result = TokenizeString(input, delimiter);

      THEN("It should return the whole string as one token") {
        REQUIRE(result.size() == expected.size());
        REQUIRE(result[0] == expected[0]);
      }
    }
  }

  GIVEN("A string with consecutive delimiters") {
    std::string_view input = "this,,is,,a,,test";
    char delimiter = ',';
    std::vector<std::string_view> expected = {"this", "", "is", "", "a", "", "test"};

    WHEN("TokenizeString is called") {
      auto result = TokenizeString(input, delimiter);

      THEN("It should handle consecutive delimiters correctly") {
        REQUIRE(result.size() == expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
          REQUIRE(result[i] == expected[i]);
        }
      }
    }
  }

  GIVEN("A string with delimiters at the start and end") {
    std::string_view input = ",this,is,a,test,";
    char delimiter = ',';
    std::vector<std::string_view> expected = {"", "this", "is", "a", "test", ""};

    WHEN("TokenizeString is called") {
      auto result = TokenizeString(input, delimiter);

      THEN("It should handle delimiters at the start and end correctly") {
        REQUIRE(result.size() == expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
          REQUIRE(result[i] == expected[i]);
        }
      }
    }
  }

  GIVEN("A string with different delimiters") {
    std::string_view input = "this|is|a|test";
    char delimiter = '|';
    std::vector<std::string_view> expected = {"this", "is", "a", "test"};

    WHEN("TokenizeString is called with a different delimiter") {
      auto result = TokenizeString(input, delimiter);

      THEN("It should split the string correctly based on the given delimiter") {
        REQUIRE(result.size() == expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
          REQUIRE(result[i] == expected[i]);
        }
      }
    }
  }
}

SCENARIO("Parsing time strings with ParseTimeString", "[ParseTimeString]") {
  using namespace PMS::Utils;

  GIVEN("A valid time string with seconds") {
    std::string_view timeStr = "30s";
    WHEN("ParseTimeString is called") {
      auto result = ParseTimeString(timeStr);
      THEN("The result should be 30 seconds") { REQUIRE(result == std::chrono::seconds(30)); }
    }
  }

  GIVEN("A valid time string with minutes") {
    std::string_view timeStr = "5m";
    WHEN("ParseTimeString is called") {
      auto result = ParseTimeString(timeStr);
      THEN("The result should be 5 minutes") { REQUIRE(result == std::chrono::minutes(5)); }
    }
  }

  GIVEN("A valid time string with hours") {
    std::string_view timeStr = "2h";
    WHEN("ParseTimeString is called") {
      auto result = ParseTimeString(timeStr);
      THEN("The result should be 2 hours") { REQUIRE(result == std::chrono::hours(2)); }
    }
  }

  GIVEN("A valid time string with days") {
    std::string_view timeStr = "1d";
    WHEN("ParseTimeString is called") {
      auto result = ParseTimeString(timeStr);
      THEN("The result should be 24 hours") { REQUIRE(result == std::chrono::hours(24)); }
    }
  }

  GIVEN("A valid time string with mixed units") {
    std::string_view timeStr = "1d2h30m45s";
    WHEN("ParseTimeString is called") {
      auto result = ParseTimeString(timeStr);
      THEN("The result should be 95445 seconds") {
        REQUIRE(result == std::chrono::seconds(95445)); // 1 day + 2 hours + 30 minutes + 45 seconds
      }
    }
  }

  GIVEN("An invalid time string with unknown unit") {
    std::string_view timeStr = "10x";
    WHEN("ParseTimeString is called") {
      THEN("An exception should be thrown with the message 'Invalid time duration: x'") {
        REQUIRE_THROWS(ParseTimeString(timeStr), "Invalid time duration: x");
      }
    }
  }

  GIVEN("An invalid time string with no units") {
    std::string_view timeStr = "123";
    WHEN("ParseTimeString is called") {
      THEN("An exception should be thrown with the message '123 is not a valid time string'") {
        REQUIRE_THROWS(ParseTimeString(timeStr), "123 is not a valid time string");
      }
    }
  }

  GIVEN("A valid time string with multiple same units") {
    std::string_view timeStr = "1h30m30m";
    WHEN("ParseTimeString is called") {
      auto result = ParseTimeString(timeStr);
      THEN("The result should be 2 hours") {
        REQUIRE(result == std::chrono::hours(2)); // 1 hour + 30 minutes + 30 minutes
      }
    }
  }

  GIVEN("A valid time string with zero values") {
    std::string_view timeStr = "0s0m0h0d";
    WHEN("ParseTimeString is called") {
      auto result = ParseTimeString(timeStr);
      THEN("The result should be 0 seconds") { REQUIRE(result == std::chrono::seconds(0)); }
    }
  }

  GIVEN("A valid time string with large values") {
    std::string_view timeStr = "1000d";
    WHEN("ParseTimeString is called") {
      auto result = ParseTimeString(timeStr);
      THEN("The result should be 24000 hours") {
        REQUIRE(result == std::chrono::hours(24000)); // 1000 days
      }
    }
  }
}
} // namespace PMS::Tests::Utils
