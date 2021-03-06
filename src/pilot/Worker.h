#ifndef PMS_PILOT_WORKER_H
#define PMS_PILOT_WORKER_H

// c++ headers
#include <map>
#include <memory>
#include <string_view>
#include <thread>
#include <utility>

// external headers
#include <boost/uuid/uuid.hpp>
#include <nlohmann/json.hpp>

// our headers
#include "common/Job.h"
#include "pilot/PilotConfig.h"
#include "pilot/client/Client.h"
#include "pilot/filetransfer/FileTransferQueue.h"

using json = nlohmann::json;
using namespace std::string_view_literals;

namespace PMS::Pilot {
class Worker {
public:
  explicit Worker(Config config, std::shared_ptr<Client> wsClient)
      : m_config{std::move(config)}, m_wsConnection{wsClient->PersistentConnection()} {}

  bool Register();

  void Start(unsigned long int maxJobs = std::numeric_limits<unsigned long int>::max());

private:
  enum class EnvInfoType { NONE, Script, List };
  std::map<EnvInfoType, std::string_view> m_envInfoNames = {{EnvInfoType::Script, "script"sv},
                                                            {EnvInfoType::List, "list"sv}};
  EnvInfoType GetEnvType(const std::string &envName);

  bool UpdateJobStatus(const std::string &hash, const std::string &task, JobStatus status);

  static std::vector<FileTransferInfo> ParseFileTransferRequest(FileTransferType, const json &, std::string_view);

  struct jobSTDIO {
    std::string stdin = "/dev/null";
    std::string stdout = "/dev/null";
    std::string stderr = "/dev/null";
  };

  std::thread m_thread;
  Config m_config;
  std::shared_ptr<Connection> m_wsConnection;

  boost::uuids::uuid m_uuid{};
};
} // namespace PMS::Pilot

#endif