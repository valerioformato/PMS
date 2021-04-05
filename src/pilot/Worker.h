#ifndef PMS_PILOT_WORKER_H
#define PMS_PILOT_WORKER_H

// c++ headers
#include <memory>
#include <thread>

// our headers
#include "db/DBHandle.h"

namespace PMS {
namespace Pilot {
class Worker {
public:
  Worker(std::shared_ptr<DB::DBHandle> handle) : m_dbhandle{handle} {}

  void Start(const std::string &user, const std::string &task = "");

private:
  std::thread m_thread;
  std::shared_ptr<DB::DBHandle> m_dbhandle;
};
} // namespace Pilot
} // namespace PMS

#endif