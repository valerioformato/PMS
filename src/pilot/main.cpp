// c++ headers
#include <functional>

// external dependencies
#include <docopt.h>
#include <spdlog/spdlog.h>

// our headers
#include "db/CredType.h"
#include "db/PoolHandle.h"
#include "pilot/PilotConfig.h"
#include "pilot/Worker.h"
#include "pilot/client/Client.h"

static constexpr auto USAGE =
    R"(PMS Pilot fish executable.

    Usage:
          PMSPilot <configfile> [ -v | -vv ] [ -m MAXJOBS ]
          PMSPilot --version
 Options:
          -v...                         Enable debug output (verbose, trace)
          -h --help                     Show this screen.
          -m MAXJOBS --maxJobs=MAXJOBS  Number of jobs to run before shutdown
          --version                     Show version.
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

  std::string serverUri = fmt::format("ws://{}:{}", config.server, config.serverPort);
  spdlog::info("Connecting to Server: {}", serverUri);
  auto wsClient = std::make_shared<PMS::Pilot::Client>(serverUri);

  unsigned long int maxJobs =
      args.find("MAXJOBS") == end(args) ? std::numeric_limits<unsigned long int>::max() : args["MAXJOBS"].asLong();

  Pilot::Worker worker{config, wsClient};
  if (!worker.Register()) {
    spdlog::warn("Served returned no valid tasks. Please check your token(s)");
    return 0;
  }
  worker.Start(maxJobs);

  return 0;
}