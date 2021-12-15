// c++ headers
#include <functional>
#include <signal.h>

// external dependencies
#include <docopt.h>
#include <spdlog/spdlog.h>

// our headers
#include "common/Utils.h"
#include "pilot/PilotConfig.h"
#include "pilot/Worker.h"
#include "pilot/client/Client.h"

static constexpr auto USAGE =
    R"(PMS Pilot fish executable.

    Usage:
          PMSPilot <configfile> [-v | -vv] [-m MAXJOBS] [-t MAXTIME]
          PMSPilot --version

    Options:
          -v...                           Enable debug output (verbose, trace)
          -h --help                       Show this screen.
          -m MAXJOBS, --maxJobs=MAXJOBS   Number of jobs to run before shutdown
          -t MAXTIME, --maxTime=MAXTIME   Time limit ("2d", "1h30m", "40s", ...)
          --version                       Show version.
)";

using namespace PMS;

// we need to do some workaround to cleanly shut down the server
// when a SIGINT or SIGTERM arrives
namespace {
volatile std::sig_atomic_t gSignalStatus;
}

void signal_handler(int signal) { gSignalStatus = signal; }

void signal_watcher(Pilot::Worker &worker) {
  while (gSignalStatus == 0) {
    std::this_thread::sleep_for(std::chrono::seconds{1});
  }

  spdlog::warn("Received signal {}", gSignalStatus);
  worker.Kill();
}

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

  // Install a signal handler
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  // read the configuration from input file
  std::string configFileName = args["<configfile>"].asString();
  const Pilot::Config config{configFileName};

  std::string serverUri = fmt::format("ws://{}", config.server);
  spdlog::info("Connecting to Server: {}", serverUri);
  auto wsClient = std::make_shared<PMS::Pilot::Client>(serverUri);

  Pilot::Worker worker{config, wsClient};
  if (!worker.Register()) {
    spdlog::warn("Served returned no valid tasks. Please check your token(s)");
    return 0;
  }

  if(args["--maxJobs"])
    worker.SetMaxJobs(args["--maxJobs"].asLong());

  if(args["--maxTime"])
    worker.SetMaxTime(Utils::ParseTimeString(args["--maxTime"].asString()));


  // Run everything!
  std::thread watchThread{signal_watcher, std::ref(worker)};
  worker.Start();

  watchThread.join();
  return 0;
}
