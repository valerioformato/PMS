#ifndef PMS_PILOT_WORKER_H
#define PMS_PILOT_WORKER_H

// c++ headers
#include <chrono>
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
#include "common/queue.h"
#include "pilot/PilotConfig.h"
#include "pilot/client/Client.h"
#include "pilot/filetransfer/FileTransferQueue.h"

using json = nlohmann::json;
using namespace std::string_view_literals;

namespace PMS::Pilot {

struct Info;

class Worker {
public:
  explicit Worker(Config config, std::unique_ptr<Client> &&wsClient)
      : m_config{std::move(config)}, m_wsClient{std::move(wsClient)}, m_wsConnection{
                                                                          m_wsClient->PersistentConnection()} {}

  ErrorOr<void> Register(const Info &);

  void Start();
  void Stop();
  void Kill();

  void SetMaxJobs(unsigned int maxJobs) { m_maxJobs = maxJobs; }

  template <class Rep, class Period> void SetMaxTime(std::chrono::duration<Rep, Period> maxTime) {
    m_maxTime = std::chrono::duration_cast<std::chrono::seconds>(maxTime);
  }

private:
  enum class State { JOB_ACQUIRED, RUN, SLEEP, WAIT, EXIT };

  Config m_config;
  std::unique_ptr<Client> m_wsClient;
  std::unique_ptr<Connection> m_wsConnection;

  State m_workerState = State::WAIT;
  std::thread m_workerThread;

  ts_queue<json> m_queuedJobUpdates;
  std::thread m_jobUpdateThread;
  void SendJobUpdates();

  unsigned long int m_maxJobs = std::numeric_limits<unsigned long int>::max();
  std::chrono::seconds m_maxTime = std::chrono::seconds::max();

  std::promise<void> m_exitSignal;
  std::shared_future<void> m_exitSignalFuture{m_exitSignal.get_future()};

  boost::process::child m_jobProcess;
  boost::uuids::uuid m_uuid{};

  void MainLoop();

  enum class EnvInfoType { NONE, Script, List };
  std::map<EnvInfoType, std::string_view> m_envInfoNames = {{EnvInfoType::Script, "script"sv},
                                                            {EnvInfoType::List, "list"sv}};
  EnvInfoType GetEnvType(const std::string &envName);

  std::map<std::chrono::system_clock::time_point, std::string> m_jobFailures;
  void UpdateJobStatus(const std::string &hash, const std::string &task, JobStatus status);

  std::vector<FileTransferInfo> ParseFileTransferRequest(FileTransferType, json, std::string_view);

  struct jobSTDIO {
    std::string stdin = "/dev/null";
    std::string stdout = "/dev/null";
    std::string stderr = "/dev/null";
  };
};
} // namespace PMS::Pilot

#endif
