#ifndef PMS_PILOT_WORKER_H
#define PMS_PILOT_WORKER_H

// c++ headers
#include <map>
#include <memory>
#include <thread>
#include <utility>

// external headers
#include <nlohmann/json.hpp>

// our headers
#include "db/PoolHandle.h"

using json = nlohmann::json;

namespace PMS {
namespace Pilot {
class Worker {
public:
  explicit Worker(std::shared_ptr<DB::PoolHandle> handle) : m_poolHandle{std::move(handle)} {}

  void Start(const std::string &user, const std::string &task = "", unsigned long int maxJobs = std::numeric_limits<unsigned long int>::max());

private:
  enum class EnvInfoType { NONE, Script, List };
  std::map<EnvInfoType, std::string> m_envInfoNames = {{EnvInfoType::Script, "script"}, {EnvInfoType::List, "list"}};
  EnvInfoType GetEnvType(const std::string &envName);

  struct jobSTDIO {
    std::string stdin;
    std::string stdout;
    std::string stderr;
  };

  std::thread m_thread;
  std::shared_ptr<DB::PoolHandle> m_poolHandle;
};
} // namespace Pilot
} // namespace PMS

#endif