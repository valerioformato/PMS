#ifndef PMS_PILOT_WORKER_H
#define PMS_PILOT_WORKER_H

// c++ headers
#include <memory>
#include <thread>
#include <utility>

// our headers
#include "db/PoolHandle.h"

namespace PMS {
namespace Pilot {
class Worker {
public:
  explicit Worker(std::shared_ptr<DB::PoolHandle> handle) : m_poolHandle{std::move(handle)} {}

  void Start(const std::string &user, const std::string &task = "");

private:
  bool CheckJobClaim(const std::string &hash, const std::string &uuid) const;

  std::thread m_thread;
  std::shared_ptr<DB::PoolHandle> m_poolHandle;
};
} // namespace Pilot
} // namespace PMS

#endif