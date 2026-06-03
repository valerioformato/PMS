// c++ headers
#include <filesystem>
#include <fstream>
#include <string>

// external dependencies
#include <catch2/catch_test_macros.hpp>
#include <nlohmann/json.hpp>

// our headers
#include "orchestrator/OrchestratorConfig.h"

using json = nlohmann::json;
using namespace PMS::Orchestrator;

namespace PMS::Tests::Orchestrator {

namespace {

// Write a JSON object to a temp file and return the path.
std::filesystem::path write_temp_config(const json &cfg) {
  auto path = std::filesystem::temp_directory_path() / "pms_test_config.json";
  std::ofstream out{path};
  out << cfg.dump();
  return path;
}

} // namespace

// ---------------------------------------------------------------------------
// Full config — all fields present
// ---------------------------------------------------------------------------

SCENARIO("OrchestratorConfig: all fields present", "[OrchestratorConfig]") {
  GIVEN("a config file with all mandatory and optional fields") {
    json cfg = {
        {"back_dbhost", "mongo://back:27017"}, {"back_dbname", "back_db"}, {"front_dbhost", "mongo://front:27017"},
        {"front_dbname", "front_db"},          {"listeningPort", 8080u},   {"nConnectionThreads", 16u},
        {"maxJobTransferQuerySize", 500u},
    };
    auto path = write_temp_config(cfg);

    WHEN("Config is constructed") {
      Config c{path.string()};

      THEN("all fields are populated correctly") {
        REQUIRE(c.back_dbhost == "mongo://back:27017");
        REQUIRE(c.back_dbname == "back_db");
        REQUIRE(c.front_dbhost == "mongo://front:27017");
        REQUIRE(c.front_dbname == "front_db");
        REQUIRE(c.listeningPort == 8080u);
        REQUIRE(c.n_connection_threads == 16u);
        REQUIRE(c.maxJobTransferQuerySize == 500u);
      }
    }

    std::filesystem::remove(path);
  }
}

// ---------------------------------------------------------------------------
// Optional fields absent — defaults apply
// ---------------------------------------------------------------------------

SCENARIO("OrchestratorConfig: optional fields absent", "[OrchestratorConfig]") {
  GIVEN("a config file without nConnectionThreads and maxJobTransferQuerySize") {
    json cfg = {
        {"back_dbhost", "mongo://back:27017"}, {"back_dbname", "back_db"}, {"front_dbhost", "mongo://front:27017"},
        {"front_dbname", "front_db"},          {"listeningPort", 9090u},
    };
    auto path = write_temp_config(cfg);

    WHEN("Config is constructed") {
      Config c{path.string()};

      THEN("optional fields retain their default values") {
        REQUIRE(c.n_connection_threads == 32u);
        REQUIRE(c.maxJobTransferQuerySize == 1000u);
      }

      AND_THEN("mandatory fields are still parsed correctly") {
        REQUIRE(c.listeningPort == 9090u);
        REQUIRE(c.back_dbhost == "mongo://back:27017");
      }
    }

    std::filesystem::remove(path);
  }
}

// ---------------------------------------------------------------------------
// Non-existent file — constructor throws
// ---------------------------------------------------------------------------

SCENARIO("OrchestratorConfig: non-existent file", "[OrchestratorConfig]") {
  GIVEN("a path that does not exist") {
    WHEN("Config is constructed") {
      THEN("it throws") { REQUIRE_THROWS(Config{"/tmp/pms_this_file_does_not_exist_xyz.json"}); }
    }
  }
}

// ---------------------------------------------------------------------------
// Missing required field — constructor throws
// ---------------------------------------------------------------------------

SCENARIO("OrchestratorConfig: missing required string field", "[OrchestratorConfig]") {
  GIVEN("a config file missing back_dbhost") {
    json cfg = {
        {"back_dbname", "back_db"},
        {"front_dbhost", "mongo://front:27017"},
        {"front_dbname", "front_db"},
        {"listeningPort", 8080u},
    };
    auto path = write_temp_config(cfg);

    WHEN("Config is constructed") {
      THEN("it throws because the string get<> on null fails") { REQUIRE_THROWS(Config{path.string()}); }
    }

    std::filesystem::remove(path);
  }
}

SCENARIO("OrchestratorConfig: missing listeningPort", "[OrchestratorConfig]") {
  GIVEN("a config file missing listeningPort") {
    json cfg = {
        {"back_dbhost", "mongo://back:27017"},
        {"back_dbname", "back_db"},
        {"front_dbhost", "mongo://front:27017"},
        {"front_dbname", "front_db"},
    };
    auto path = write_temp_config(cfg);

    WHEN("Config is constructed") {
      THEN("it throws because get<unsigned int> on null fails") { REQUIRE_THROWS(Config{path.string()}); }
    }

    std::filesystem::remove(path);
  }
}

// ---------------------------------------------------------------------------
// Wrong type for listeningPort
// ---------------------------------------------------------------------------

SCENARIO("OrchestratorConfig: wrong type for listeningPort", "[OrchestratorConfig]") {
  GIVEN("a config file where listeningPort is a string instead of an integer") {
    json cfg = {
        {"back_dbhost", "mongo://back:27017"},   {"back_dbname", "back_db"},
        {"front_dbhost", "mongo://front:27017"}, {"front_dbname", "front_db"},
        {"listeningPort", "not-a-number"},
    };
    auto path = write_temp_config(cfg);

    WHEN("Config is constructed") {
      THEN("it throws on the type mismatch") { REQUIRE_THROWS(Config{path.string()}); }
    }

    std::filesystem::remove(path);
  }
}

// ---------------------------------------------------------------------------
// Invalid JSON syntax
// ---------------------------------------------------------------------------

SCENARIO("OrchestratorConfig: invalid JSON syntax in file", "[OrchestratorConfig]") {
  GIVEN("a file containing unparseable text") {
    auto path = std::filesystem::temp_directory_path() / "pms_test_bad_config.json";
    {
      std::ofstream out{path};
      out << "{this is not json}";
    }

    WHEN("Config is constructed") {
      THEN("it throws a JSON parse error") { REQUIRE_THROWS(Config{path.string()}); }
    }

    std::filesystem::remove(path);
  }
}

} // namespace PMS::Tests::Orchestrator
