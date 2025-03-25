// c++ headers
#include <filesystem>
#include <fstream>

// external dependencies
#include <nlohmann/json.hpp>

// our headers
#include "common/JsonUtils.h"
#include "orchestrator/OrchestratorConfig.h"

using json = nlohmann::json;
using namespace PMS::JsonUtils;

namespace PMS::Orchestrator {
Config::Config(std::string_view fileName) {
  std::ifstream infile{std::filesystem::path{fileName}};
  json configJson;
  infile >> configJson;

  back_dbhost = to_string(configJson["back_dbhost"]);
  back_dbname = to_string(configJson["back_dbname"]);

  front_dbhost = to_string(configJson["front_dbhost"]);
  front_dbname = to_string(configJson["front_dbname"]);
  // front_dbuser = configJson["front_dbuser"];

  listeningPort = configJson["listeningPort"].get<unsigned int>();

  if (configJson.contains("nConnectionThreads")) {
    nConnectionThreads = configJson["nConnectionThreads"].get<unsigned int>();
  }

  if (configJson.contains("maxJobTransferQuerySize")) {
    maxJobTransferQuerySize = configJson["maxJobTransferQuerySize"].get<unsigned int>();
  }
}
} // namespace PMS::Orchestrator
