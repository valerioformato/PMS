// c++ headers
#include <chrono>
#include <csignal>
#include <functional>

// external dependencies
#include <docopt.h>
#include <spdlog/spdlog.h>

// our headers
#include "PMSVersion.h"
#include "db/CredType.h"
#include "db/backends/MongoDB/PoolHandle.h"
#include "orchestrator/Director.h"
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

// we need to do some workaround to cleanly shut down the server
// when a SIGINT or SIGTERM arrives
namespace {
volatile std::sig_atomic_t gSignalStatus;
}

void signal_handler(int signal) { gSignalStatus = signal; }

void signal_watcher(Orchestrator::Server &server) {
  while (gSignalStatus == 0) {
    std::this_thread::sleep_for(std::chrono::seconds{1});
  }

  spdlog::warn("Received signal {}", gSignalStatus);
  server.Stop();
}

int main(int argc, const char **argv) {
  std::map<std::string, docopt::value> args =
      docopt::docopt(USAGE, {std::next(argv), std::next(argv, argc)},
                     true, // show help if requested
                     fmt::format("PMS {} ({})", PMS::Version::AsString(), PMS::Version::git_sha)); // version string

  switch (args["-v"].asLong()) {
  case 1:
    spdlog::set_level(spdlog::level::debug);
    break;
  case 2:
    spdlog::set_level(spdlog::level::trace);
    break;
  }

  spdlog::set_pattern("[%D %T] %-12n [%^%l%$] %v");

  // Use the default logger (stdout, multi-threaded, colored)
  spdlog::info("Starting orchestrator");

  // Install a signal handler
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  // read the configuration from input file
  std::string configFileName = args["<configfile>"].asString();
  const Orchestrator::Config config{configFileName};

  auto director = std::make_shared<Orchestrator::Director>();
  director->SetFrontDB(config.front_dbhost, config.front_dbname);
  director->SetBackDB(config.back_dbhost, config.back_dbname);
  director->SetMaxJobTransferQuerySize(config.maxJobTransferQuerySize);

  Orchestrator::Server server{config.listeningPort, director, config.nConnectionThreads};

  // prepare to run everything...
  std::vector<std::thread> threads;

  // pass the server to the signal watcher so it can be cleanly shutdown if the process is
  // interrupted
  threads.emplace_back(signal_watcher, std::ref(server));

  // run the websocket server in a dedicated thread
  threads.emplace_back([](Orchestrator::Server &_server) { _server.Start(); }, std::ref(server));

  // start the director
  director->Start();

  // finishing...
  std::for_each(begin(threads), end(threads), [](std::thread &thread) { thread.join(); });

  // stop all director operations
  director->Stop();

  return 0;
}
