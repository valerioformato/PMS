// c++ headers
#include <fstream>

// external dependencies
#include <fmt/format.h>
#include <nlohmann/json.hpp>

// our headers
#include "pilot/PilotConfig.h"

using json = nlohmann::json;

namespace PMS {
namespace Pilot {
Config::Config(std::string fileName) {
  std::ifstream infile(fileName);
  json configJson;
  infile >> configJson;

  user = configJson["user"];
  task = configJson["task"];
  dbhost = configJson["dbhost"];
  dbname = configJson["dbname"];
  dbuser = configJson["dbuser"];

  if (configJson["dbcredtype"] == "password") {
    dbcredtype = DB::CredType::PWD;
  } else if (configJson["dbcredtype"] == "X509") {
    dbcredtype = DB::CredType::X509;
  } else {
    dbcredtype = DB::CredType::None;
  }
  dbcredentials = configJson["dbcredentials"];
}
} // namespace Pilot
} // namespace PMS
