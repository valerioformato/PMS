// c++ headers
#include <chrono>
#include <csignal>
#include <cstdio>
#include <functional>

// external dependencies
#include <cxxopts.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "PMSVersion.h"
#include "db/CredType.h"
#include "orchestrator/Director.h"
#include "orchestrator/OrchestratorConfig.h"
#include "orchestrator/Server.h"

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
  cxxopts::Options options{"PMSOrchestrator", "PMS job orchestrator executable."};
  options.positional_help("<configfile> [options]");
  options.add_options()("configfile", "Configuration file", cxxopts::value<std::string>())(
      "v,verbose", "Enable debug output (repeat for trace)")("h,help", "Show this screen")("version", "Show version");
  options.parse_positional({"configfile"});

  cxxopts::ParseResult args;
  try {
    args = options.parse(argc, argv);
  } catch (const cxxopts::exceptions::exception &error) {
    fmt::print(stderr, "Error parsing options: {}\n\n{}\n", error.what(), options.help());
    return 1;
  }

  if (args.count("help") != 0) {
    fmt::print("{}\n", options.help());
    return 0;
  }

  if (args.count("version") != 0) {
    fmt::print("PMS {} ({})\n", PMS::Version::AsString(), PMS::Version::git_sha);
    return 0;
  }

  if (!args.unmatched().empty()) {
    fmt::print(stderr, "Unexpected argument: {}\n\n{}\n", args.unmatched().front(), options.help());
    return 1;
  }

  if (args.count("configfile") == 0) {
    fmt::print(stderr, "Missing required argument: <configfile>\n\n{}\n", options.help());
    return 1;
  }

  switch (args.count("verbose")) {
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
  std::string configFileName = args["configfile"].as<std::string>();
  const Orchestrator::Config config{configFileName};

  auto director = std::make_shared<Orchestrator::Director>(config.n_IO_threads);
  director->SetFrontDB(config.front_dbhost, config.front_dbname);
  director->SetBackDB(config.back_dbhost, config.back_dbname);
  director->SetMaxJobTransferQuerySize(config.maxJobTransferQuerySize);

  Orchestrator::Server server{config.listeningPort, director, config.n_connection_threads};

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
