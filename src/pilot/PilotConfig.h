#ifndef PMS_PILOT_CONFIG_H
#define PMS_PILOT_CONFIG_H

// c++ headers
#include <string>
#include <vector>

namespace PMS {
namespace Pilot {
struct Config {
  explicit Config(const std::string& fileName);

  std::string user;
  std::vector<std::pair<std::string, std::string>> tasks;
  std::string server;
  unsigned int serverPort = 9003;
  std::vector<std::string> tokens;
};
} // namespace Pilot
} // namespace PMS

#endif