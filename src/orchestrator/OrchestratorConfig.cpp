// c++ headers
#include <fstream>

// external dependencies
#include <fmt/format.h>
#include <nlohmann/json.hpp>

// our headers
#include "orchestrator/OrchestratorConfig.h"

using json = nlohmann::json;

namespace PMS {
namespace Orchestrator {
Config::Config(std::string fileName) {
  std::ifstream infile(fileName);
  json configJson;
  infile >> configJson;

  dbhost = configJson["dbhost"];
  dbname = configJson["dbname"];
  dbuser = configJson["dbuser"];

  if (configJson["dbcredtype"] == "password") {
    dbcredtype = CredType::PWD;
  } else if (configJson["dbcredtype"] == "X509") {
    dbcredtype = CredType::X509;
  } else {
    dbcredtype = CredType::None;
  }
  dbcredentials = configJson["dbcredentials"];
}
} // namespace Orchestrator
} // namespace PMS
