// c++ headers
#include <fstream>

// external dependencies
#include <nlohmann/json.hpp>

// our headers
#include "pilot/PilotConfig.h"

using json = nlohmann::json;

namespace PMS::Pilot {
Config::Config(const std::string &fileName) {
  std::ifstream infile(fileName);
  json configJson;
  infile >> configJson;

  user = configJson["user"];
  server = configJson["server"];
  if (configJson.contains("serverPort"))
    serverPort = configJson["serverPort"];

  auto dummy = configJson["tokens"];
  std::copy(dummy.begin(), dummy.end(), std::back_inserter(tokens));

  dummy = configJson["tasks"];
  std::for_each(dummy.begin(), dummy.end(), [this](auto doc) { tasks.emplace_back(doc["name"], doc["token"]); });
}
} // namespace PMS::Pilot
