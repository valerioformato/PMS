// c++ headers
#include <functional>
#include <iostream>

// external dependencies
#include <docopt.h>
#include <spdlog/spdlog.h>

// our headers
#include "db/PoolHandle.h"
#include "pilot/PilotConfig.h"
#include "pilot/Worker.h"

static constexpr auto USAGE =
    R"(PMS Pilot fish executable.

    Usage:
          PMSPilot <configfile> [ -v | -vv ]
          PMSPilot --version
 Options:
          -v...         Enable debug output (verbose, trace)
          -h --help     Show this screen.
          --version     Show version.
)";

using namespace PMS;

int main(int argc, const char **argv) {
  std::map<std::string, docopt::value> args = docopt::docopt(USAGE, {std::next(argv), std::next(argv, argc)},
                                                             true,         // show help if requested
                                                             "PMS 0.0.1"); // version string

  switch (args["-v"].asLong()) {
  case 1:
    spdlog::set_level(spdlog::level::debug);
    break;
  case 2:
    spdlog::set_level(spdlog::level::trace);
    break;
  }

  // Use the default logger (stdout, multi-threaded, colored)
  spdlog::info("Starting pilot job");

  // read the configuration from input file
  std::string configFileName = args["<configfile>"].asString();
  const Pilot::Config config{configFileName};

  spdlog::info("Connecting to DB: {}@{}/{}", config.dbuser, config.dbhost, config.dbname);
  std::shared_ptr<PMS::DB::PoolHandle> poolHandle;
  switch (config.dbcredtype) {
  case Pilot::CredType::PWD:
    poolHandle =
        std::make_shared<PMS::DB::PoolHandle>(config.dbhost, config.dbname, config.dbuser, config.dbcredentials);
    break;
  case Pilot::CredType::X509:
    // TODO: Figure out how X509 credentials propagate
    throw std::runtime_error("X509 credentials not supported yet");
    break;
  default:
    break;
  }

  Pilot::Worker worker{poolHandle};
  worker.Start(config.user, config.task);

  return 0;
}