#ifndef PMS_PILOT_WORKER_H
#define PMS_PILOT_WORKER_H

// c++ headers
#include <map>
#include <memory>
#include <string_view>
#include <thread>
#include <utility>

// external headers
#include <boost/process.hpp>
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
  ~Worker() { m_workerThread.join(); }

  bool Register();

  void Start();
  void Kill();

  void SetMaxJobs(unsigned int maxJobs) { m_maxJobs = maxJobs; }

private:
  enum class State { JOBACQUIRED, RUN, SLEEP, WAIT, EXIT };

  State m_workerState = State::WAIT;
  std::thread m_workerThread;
  Config m_config;
  std::shared_ptr<Connection> m_wsConnection;
  unsigned long int m_maxJobs = std::numeric_limits<unsigned long int>::max();

  std::promise<void> m_exitSignal;
  std::shared_future<void> m_exitSignalFuture{m_exitSignal.get_future()};

  boost::process::child m_jobProcess;
  boost::uuids::uuid m_uuid{};

  void MainLoop();

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
};
} // namespace PMS::Pilot

#endif