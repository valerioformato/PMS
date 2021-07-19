#ifndef PMS_ORCHESTRATOR_CONFIG_H
#define PMS_ORCHESTRATOR_CONFIG_H

// c++ headers
#include <string>

// our headers
#include "db/CredType.h"

namespace PMS {
namespace Orchestrator {
enum class CredType {
  None = 0,
  PWD,
  X509,
};

struct Config {
  explicit Config(const std::string& fileName);

  std::string back_dbhost;
  std::string back_dbname;

  std::string front_dbhost;
  std::string front_dbname;
  std::string front_dbuser;

  DB::CredType front_dbcredtype;
  std::string front_dbcredentials;

  unsigned int listeningPort = 0;
};
} // namespace Orchestrator
} // namespace PMS

#endif