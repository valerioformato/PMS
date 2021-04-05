#ifndef PMS_PILOT_WORKER_H
#define PMS_PILOT_WORKER_H

// c++ headers
#include <memory>
#include <thread>

// our headers
#include "db/PoolHandle.h"

namespace PMS {
namespace Pilot {
class Worker {
public:
  Worker(std::shared_ptr<DB::PoolHandle> handle) : m_poolHandle{handle} {}

  void Start(const std::string &user, const std::string &task = "");

private:
  std::thread m_thread;
  std::shared_ptr<DB::PoolHandle> m_poolHandle;
};
} // namespace Pilot
} // namespace PMS

#endif