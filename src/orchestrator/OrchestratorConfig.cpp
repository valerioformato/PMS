// c++ headers
#include <filesystem>
#include <fstream>

// external dependencies
#include <nlohmann/json.hpp>

// our headers
#include "orchestrator/OrchestratorConfig.h"

using json = nlohmann::json;

namespace PMS::Orchestrator {
Config::Config(std::string_view fileName) {
  std::ifstream infile{std::filesystem::path{fileName}};
  json configJson;
  infile >> configJson;

  back_dbhost = configJson["back_dbhost"];
  back_dbname = configJson["back_dbname"];

  front_dbhost = configJson["front_dbhost"];
  front_dbname = configJson["front_dbname"];
  // front_dbuser = configJson["front_dbuser"];

  // if (configJson["front_dbcredtype"] == "password") {
  //   front_dbcredtype = DB::CredType::PWD;
  // } else if (configJson["front_dbcredtype"] == "X509") {
  //   front_dbcredtype = DB::CredType::X509;
  // } else {
  //   front_dbcredtype = DB::CredType::None;
  // }
  // front_dbcredentials = configJson["front_dbcredentials"];

  listeningPort = configJson["listeningPort"];
}
} // namespace PMS::Orchestrator
