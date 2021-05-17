// c++ headers
#include <chrono>
#include <csignal>
#include <functional>

// external dependencies
#include <docopt.h>
#include <spdlog/spdlog.h>

// our headers
#include "db/PoolHandle.h"
#include "orchestrator/OrchestratorConfig.h"
#include "orchestrator/Server.h"

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

namespace {
volatile std::sig_atomic_t gSignalStatus;
}

void signal_handler(int signal) {
  gSignalStatus = signal;
}

void signal_watcher(Orchestrator::Server &server) {
  while (gSignalStatus == 0) {
    std::this_thread::sleep_for(std::chrono::seconds{1});
  }

  spdlog::warn("Received signal {}", gSignalStatus);
  server.Stop();
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
  spdlog::info("Starting orchestrator");

  // Install a signal handler
  std::signal(SIGINT, signal_handler);
  std::signal(SIGKILL, signal_handler);

  // read the configuration from input file
  std::string configFileName = args["<configfile>"].asString();
  const Orchestrator::Config config{configFileName};

  // spdlog::info("Connecting to DB: {}@{}/{}", config.dbuser, config.dbhost, config.dbname);
  // std::shared_ptr<PMS::DB::PoolHandle> poolHandle;

  Orchestrator::Server server{config.listeningPort};

  // pass the server to the signal watcher so it can be cleanly shutdown if the process is
  // interrupted
  std::thread watchThread{signal_watcher, std::ref(server)};

  server.Start();

  watchThread.join();

  return 0;
}