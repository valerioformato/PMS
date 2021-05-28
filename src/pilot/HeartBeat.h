#ifndef PMS_PILOT_HEARTBEAT_H
#define PMS_PILOT_HEARTBEAT_H

// c++ headers
#include <future>
#include <memory>
#include <thread>

// external headers
#include <boost/uuid/random_generator.hpp>
#include <utility>
#include <boost/uuid/uuid.hpp>

// our headers
#include "db/PoolHandle.h"

namespace PMS {
namespace Pilot {
class HeartBeat {
public:
  HeartBeat(boost::uuids::uuid uuid, std::shared_ptr<DB::PoolHandle> handle)
      : m_uuid{uuid}, m_poolHandle{std::move(handle)}, m_exitSignal{}, m_thread{&HeartBeat::updateHB, this,
                                                                     m_exitSignal.get_future()} {}
  ~HeartBeat();

private:
  boost::uuids::uuid m_uuid;
  std::shared_ptr<DB::PoolHandle> m_poolHandle;
  std::promise<void> m_exitSignal;
  std::thread m_thread;

  void updateHB(std::future<void> exitSignal);
};
} // namespace Pilot
} // namespace PMS
#endif