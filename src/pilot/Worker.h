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

using json = nlohmann::json;
using namespace std::string_view_literals;

namespace PMS::Pilot {
class Worker {
public:
  explicit Worker(Config config, std::shared_ptr<Client> wsClient)
      : m_config{std::move(config)}, m_wsClient{std::move(wsClient)} {}

  bool Register();

  void Start(unsigned long int maxJobs = std::numeric_limits<unsigned long int>::max());

private:
  enum class EnvInfoType { NONE, Script, List };
  std::map<EnvInfoType, std::string_view> m_envInfoNames = {{EnvInfoType::Script, "script"sv},
                                                            {EnvInfoType::List, "list"sv}};
  EnvInfoType GetEnvType(const std::string &envName);

  bool UpdateJobStatus(const std::string &hash, const std::string &task, JobStatus status);

  enum class FileTransferType { Inbound, Outbound };
  enum class FileTransferProtocol { local, xrootd };
  struct FileTransferInfo {
    FileTransferType type;
    FileTransferProtocol protocol;
    std::string fileName;
    std::string remotePath;
    std::string currentPath;
  };
  bool FileTransfer(const FileTransferInfo &ftInfo);
  static std::vector<FileTransferInfo> ParseFileTransferRequest(FileTransferType, const json &, std::string_view);
  static bool LocalFileTransfer(const FileTransferInfo &ftInfo);


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
} // namespace PMS::Pilot

#endif