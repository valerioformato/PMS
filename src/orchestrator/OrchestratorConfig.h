#ifndef PMS_ORCHESTRATOR_CONFIG_H
#define PMS_ORCHESTRATOR_CONFIG_H

// c++ headers
#include <string>

namespace PMS {
namespace Orchestrator {
enum class CredType {
  None = 0,
  PWD,
  X509,
};

struct Config {
  Config(std::string fileName);

  std::string dbhost;
  std::string dbname;
  std::string dbuser;

  CredType dbcredtype;
  std::string dbcredentials; // TODO: Figure out how to log to the DB, which credentials to be used?
};
} // namespace Orchestrator
} // namespace PMS

#endif