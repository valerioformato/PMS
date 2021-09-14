#ifndef PMS_PILOT_HEARTBEAT_H
#define PMS_PILOT_HEARTBEAT_H

// c++ headers
#include <future>
#include <memory>
#include <thread>

// external headers
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <utility>

// our headers
#include "pilot/client/Connection.h"

namespace PMS::Pilot {
class HeartBeat {
public:
  HeartBeat(boost::uuids::uuid uuid, std::shared_ptr<Connection> wsConnection)
      : m_uuid{uuid}, m_wsConnection{std::move(wsConnection)}, m_exitSignal{}, m_thread{&HeartBeat::updateHB, this,
                                                                                m_exitSignal.get_future()} {}
  ~HeartBeat();

  [[nodiscard]] bool IsAlive() const { return m_alive; }

private:
  boost::uuids::uuid m_uuid;
  std::shared_ptr<Connection> m_wsConnection;
  std::promise<void> m_exitSignal;
  std::thread m_thread;
  bool m_alive = false;

  void updateHB(std::future<void> exitSignal);
};
} // namespace PMS::Pilot
#endif