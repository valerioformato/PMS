#ifndef PMS_PILOT_HEARTBEAT_H
#define PMS_PILOT_HEARTBEAT_H

// c++ headers
#include <future>
#include <memory>
#include <thread>

// our headers
#include "db/DBHandle.h"

namespace PMS {
namespace Pilot {
class HeartBeat {
public:
  HeartBeat(std::shared_ptr<DB::DBHandle> handle)
      : m_dbhandle{handle}, m_exitSignal{}, m_thread{&HeartBeat::updateHB, this, m_exitSignal.get_future()} {}
  ~HeartBeat();

private:
  std::shared_ptr<DB::DBHandle> m_dbhandle;
  std::promise<void> m_exitSignal;
  std::thread m_thread;

  void updateHB(std::future<void> exitSignal);
};
} // namespace Pilot
} // namespace PMS
#endif