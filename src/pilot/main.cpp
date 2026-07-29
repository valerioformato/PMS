// c++ headers
#include <cstdio>
#include <fstream>
#include <functional>
#include <signal.h>

// external dependencies
#include <boost/asio/ip/address.hpp>
#include <cxxopts.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "PMSVersion.h"
#include "common/Utils.h"
#include "pilot/PilotConfig.h"
#include "pilot/PilotInfo.h"
#include "pilot/Worker.h"
#include "pilot/client/Client.h"

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

ErrorOr<std::string> read_file(const std::filesystem::path &file) {
  std::ifstream is(file);
  if (!is.good()) {
    return make_error(std::make_error_code(std::errc::no_such_file_or_directory),
                      fmt::format("File {} not found", file.string()));
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

  auto get_ip_address = []() -> std::string {
    boost::asio::io_context ioService;
    boost::asio::ip::tcp::resolver resolver(ioService);

    auto results = resolver.resolve(boost::asio::ip::host_name(), "");
    for (const auto &result : results) {
      // FIXME: we only return the first one, what if there's more than one?
      return result.endpoint().address().to_string();
    }

    return {};
  };

  result.hostname = boost::asio::ip::host_name();
  result.ip = get_ip_address();

  auto os_version_or_error = read_file("/proc/version");
  result.os_version = os_version_or_error ? os_version_or_error.value() : "Unknown";

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
  cxxopts::Options options{"PMSPilot", "PMS Pilot fish executable."};
  options.positional_help("<configfile> [options]");
  options.add_options()("configfile", "Configuration file",
                        cxxopts::value<std::string>())("v,verbose", "Enable debug output (repeat for trace)")(
      "m,maxJobs", "Number of jobs to run before shutdown", cxxopts::value<unsigned int>(),
      "MAXJOBS")("t,maxTime", R"(Time limit ("2d", "1h30m", "40s", ...))", cxxopts::value<std::string>(),
                 "MAXTIME")("h,help", "Show this screen")("version", "Show version");
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

  // Install a signal handler
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  Pilot::Info pilotInfo = ReadPilotInfo();
  Report(pilotInfo);

  spdlog::info("Starting pilot job");

  // read the configuration from input file
  std::string configFileName = args["configfile"].as<std::string>();
  const Pilot::Config config{configFileName};

  std::string serverUri = fmt::format("ws://{}", config.server);
  spdlog::info("Connecting to Server: {}", serverUri);

  Pilot::Worker worker{config, std::make_unique<PMS::Pilot::Client>(serverUri)};
  if (!worker.Register(pilotInfo)) {
    spdlog::warn("Served returned no valid tasks. Please check your token(s)");
    return 0;
  }

  if (args.count("maxJobs") != 0)
    worker.SetMaxJobs(args["maxJobs"].as<unsigned int>());

  if (args.count("maxTime") != 0)
    worker.SetMaxTime(Utils::ParseTimeString(args["maxTime"].as<std::string>()));

  // Run everything!
  std::thread watchThread{signal_watcher, std::ref(worker)};
  worker.Start();

  // terrible hack to stop signal handler thread
  gSignalStatus = -1;
  watchThread.join();
  return 0;
}
