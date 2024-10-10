// c++ headers
#include <filesystem>
#include <fstream>
#include <ranges>

// external dependencies
#include <nlohmann/json.hpp>

// our headers
#include "common/JsonUtils.h"
#include "pilot/PilotConfig.h"

using json = nlohmann::json;
using namespace PMS::JsonUtils;

namespace PMS::Pilot {
Config::Config(const std::string &fileName) {
  std::ifstream infile{std::filesystem::path{fileName}};
  json configJson;
  infile >> configJson;

  user = to_s(configJson["user"]);
  server = to_s(configJson["server"]);

  auto dummy = configJson["tasks"];
  std::for_each(dummy.begin(), dummy.end(), [this](auto doc) { tasks.emplace_back(doc["name"], doc["token"]); });

  dummy = configJson["tags"];
  std::ranges::transform(dummy, std::back_inserter(tags), [](const auto &tag) { return to_s(tag); });
}
} // namespace PMS::Pilot
