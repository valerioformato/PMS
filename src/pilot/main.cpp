// c++ headers
#include <functional>
#include <iostream>

// external dependencies
#include <spdlog/spdlog.h>
#include <docopt.h>
#include <nlohmann/json.hpp>
// shitload of mongodb driver headers
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/instance.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>

static constexpr auto USAGE =
  R"(PMS Pilot fish executable.

    Usage:
          PMSPilot <configfile>
          PMSPilot --version
 Options:
          -h --help     Show this screen.
          --version     Show version.
)";

int main(int argc, const char **argv)
{
  std::map<std::string, docopt::value> args = docopt::docopt(USAGE,
    { std::next(argv), std::next(argv, argc) },
    true,// show help if requested
    "PMS 0.0.1");// version string

  //Use the default logger (stdout, multi-threaded, colored)
  spdlog::info("Hello, {}!", "World");  
}