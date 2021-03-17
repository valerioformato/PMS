// c++ headers
#include <fstream>

// external dependencies
#include <fmt/format.h>
#include <nlohmann/json.hpp>

// our headers
#include "pilot/PilotConfig.h"

namespace PMS {
namespace Pilot {
Config::Config(std::string fileName) {
  std::ifstream infile(fileName);
  nlohmann::json configJson;
  infile >> configJson;

  user = configJson["user"];
  task = configJson["task"];
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
} // namespace Pilot
} // namespace PMS
