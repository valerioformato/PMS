// c++ headers
#include <functional>
#include <iostream>

// external dependencies
#include <docopt.h>
#include <spdlog/spdlog.h>

// our headers
#include "db/PoolHandle.h"
#include "orchestrator/OrchestratorConfig.h"

static constexpr auto USAGE =
    R"(PMS job orchestrator executable.

    Usage:
          PMSOrchestrator <configfile> [ -v | -vv ]
          PMSOrchestrator --version
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
  spdlog::info("Starting orchestrator");

  // read the configuration from input file
  std::string configFileName = args["<configfile>"].asString();
  const Orchestrator::Config config{configFileName};

  spdlog::info("Connecting to DB: {}@{}/{}", config.dbuser, config.dbhost, config.dbname);
  std::shared_ptr<PMS::DB::PoolHandle> poolHandle;


  return 0;
}