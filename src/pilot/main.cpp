#include <functional>
#include <iostream>

#include <spdlog/spdlog.h>

#include <docopt.h>

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