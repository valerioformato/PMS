#ifndef PMS_PILOT_CONFIG_H
#define PMS_PILOT_CONFIG_H

// c++ headers
#include <string>

// our headers
#include "db/CredType.h"

namespace PMS {
namespace Pilot {
struct Config {
  Config(std::string fileName);

  std::string user;
  std::string task;
  std::string dbhost;
  std::string dbname;
  std::string dbuser;

  DB::CredType dbcredtype;
  std::string dbcredentials; // TODO: Figure out how to log to the DB, which credentials to be used?
};
} // namespace Pilot
} // namespace PMS

#endif