// c++ headers
#include <fstream>
#include <functional>
#include <signal.h>

// external dependencies
#include <boost/asio/ip/address.hpp>
#include <docopt.h>
#include <spdlog/spdlog.h>

// our headers
#include "PMSVersion.h"
#include "common/Utils.h"
#include "pilot/PilotConfig.h"
#include "pilot/PilotInfo.h"
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

  if (gSignalStatus > 0) {
    spdlog::warn("Received signal {}", gSignalStatus);
    worker.Kill();
  }
}

std::string read_file(std::string const &file) {
  std::ifstream is(file);
  if (!is.good()) {
    throw std::runtime_error("Error: stream has errors.");
  }
  std::stringstream ss;
  ss << is.rdbuf();
  std::string m;
  // Remove ending line character '\n' or '\r\n'.
  std::getline(ss, m);
  return m;
}

Pilot::Info ReadPilotInfo() {
  Pilot::Info result{};

  auto get_ip_address = []() {
    boost::asio::io_service ioService;
    boost::asio::ip::tcp::resolver resolver(ioService);

    return resolver.resolve(boost::asio::ip::host_name(), "")->endpoint().address().to_string();
  };

  result.hostname = boost::asio::ip::host_name();
  result.ip = get_ip_address();
  try {
    result.os_version = read_file("/proc/version");
  } catch (const std::runtime_error &e) {
    // nothing :)
  }

  return result;
}

void Report(const Pilot::Info &info) {
  fmt::print("{:=^80}\n", " PMS ");
  fmt::print(" Version {} ({})\n", PMS::Version::AsString(), PMS::Version::git_sha);
  fmt::print(" This is pilot {}\n", boost::uuids::to_string(info.uuid));
  fmt::print(" Running on host {} \n IP addr: {}\n", info.hostname, info.ip);
  if (!info.os_version.empty()) {
    fmt::print(" OS Version: {}\n", info.os_version);
  }
  fmt::print("{:=^80}\n", "");
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

  // Install a signal handler
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  Pilot::Info pilotInfo = ReadPilotInfo();
  Report(pilotInfo);

  spdlog::info("Starting pilot job");

  // read the configuration from input file
  std::string configFileName = args["<configfile>"].asString();
  const Pilot::Config config{configFileName};

  std::string serverUri = fmt::format("ws://{}", config.server);
  spdlog::info("Connecting to Server: {}", serverUri);
  auto wsClient = std::make_shared<PMS::Pilot::Client>(serverUri);

  Pilot::Worker worker{config, wsClient};
  if (!worker.Register(pilotInfo)) {
    spdlog::warn("Served returned no valid tasks. Please check your token(s)");
    return 0;
  }

  if (args["--maxJobs"])
    worker.SetMaxJobs(args["--maxJobs"].asLong());

  if (args["--maxTime"])
    worker.SetMaxTime(Utils::ParseTimeString(args["--maxTime"].asString()));

  // Run everything!
  std::thread watchThread{signal_watcher, std::ref(worker)};
  worker.Start();

  // this blocks until the main loop thread has exited
  worker.Stop();

  // terrible hack to stop signal handler thread
  gSignalStatus = -1;
  watchThread.join();
  return 0;
}
