#ifndef PMS_PILOT_CONFIG_H
#define PMS_PILOT_CONFIG_H

// c++ headers
#include <string>
#include <vector>

namespace PMS::Pilot {
struct Config {
  explicit Config(const std::string &fileName);

  std::string user;
  std::vector<std::pair<std::string, std::string>> tasks;
  std::vector<std::string> tags;
  std::string server;
};
} // namespace PMS::Pilot

#endif
