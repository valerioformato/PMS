#ifndef PMS_PILOT_WORKER_H
#define PMS_PILOT_WORKER_H

// c++ headers
#include <map>
#include <memory>
#include <thread>
#include <utility>

// external headers
#include <boost/uuid/uuid.hpp>
#include <nlohmann/json.hpp>

// our headers
#include "pilot/PilotConfig.h"
#include "pilot/client/Client.h"

using json = nlohmann::json;

namespace PMS {
namespace Pilot {
class Worker {
public:
  explicit Worker(Config config, std::shared_ptr<Client> wsClient)
      : m_config{std::move(config)}, m_wsClient{std::move(wsClient)} {}

  bool Register();

  void Start(unsigned long int maxJobs = std::numeric_limits<unsigned long int>::max());

private:
  enum class EnvInfoType { NONE, Script, List };
  std::map<EnvInfoType, std::string> m_envInfoNames = {{EnvInfoType::Script, "script"}, {EnvInfoType::List, "list"}};
  EnvInfoType GetEnvType(const std::string &envName);

  bool UpdateJobStatus(const std::string &hash, const std::string &task, JobStatus status);

  struct jobSTDIO {
    std::string stdin;
    std::string stdout;
    std::string stderr;
  };

  std::thread m_thread;
  Config m_config;
  std::shared_ptr<Client> m_wsClient;

  boost::uuids::uuid m_uuid{};
};
} // namespace Pilot
} // namespace PMS

#endif