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
#include "pilot/client/Client.h"

namespace PMS::Pilot {
class HeartBeat {
public:
  HeartBeat(boost::uuids::uuid uuid, std::shared_ptr<Client> wsClient)
      : m_uuid{uuid}, m_wsClient{std::move(wsClient)}, m_exitSignal{}, m_thread{&HeartBeat::updateHB, this,
                                                                                m_exitSignal.get_future()} {}
  ~HeartBeat();

private:
  boost::uuids::uuid m_uuid;
  std::shared_ptr<Client> m_wsClient;
  std::promise<void> m_exitSignal;
  std::thread m_thread;

  void updateHB(std::future<void> exitSignal);
};
} // namespace PMS::Pilot
#endif