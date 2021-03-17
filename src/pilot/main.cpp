// c++ headers
#include <functional>
#include <iostream>

// external dependencies
#include <docopt.h>
// #include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "db/DBHandle.h"
#include "pilot/PilotConfig.h"

static constexpr auto USAGE =
    R"(PMS Pilot fish executable.

    Usage:
          PMSPilot <configfile>
          PMSPilot --version
 Options:
          -h --help     Show this screen.
          --version     Show version.
)";

using namespace PMS;

int main(int argc, const char **argv) {
  std::map<std::string, docopt::value> args = docopt::docopt(USAGE, {std::next(argv), std::next(argv, argc)},
                                                             true,         // show help if requested
                                                             "PMS 0.0.1"); // version string

  // Use the default logger (stdout, multi-threaded, colored)
  spdlog::info("Starting pilot job");

  // read the configuration from input file
  std::string configFileName = args["<configfile>"].asString();
  const Pilot::Config config{configFileName};

  spdlog::info("Connecting to DB: {}@{}", config.dbuser, config.dbhost);
  std::shared_ptr<PMS::DB::DBHandle> dbHandle;
  switch (config.dbcredtype) {
  case Pilot::CredType::PWD:
    dbHandle = std::make_shared<PMS::DB::DBHandle>(config.dbhost, config.dbname, config.dbuser, config.dbcredentials);
    break;
  case Pilot::CredType::X509:
    // TODO: Figure out how X509 credentials propagate
    throw std::runtime_error("X509 credentials not supported yet");
    break;
  default:
    break;
  }

  return 0;
}