#ifndef PMS_PILOT_HEARTBEAT_H
#define PMS_PILOT_HEARTBEAT_H

// c++ headers
#include <memory>
#include <stop_token>
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
  HeartBeat(boost::uuids::uuid uuid, std::unique_ptr<Connection> wsConnection)
      : m_uuid{uuid}, m_wsConnection{std::move(wsConnection)}, m_thread{&HeartBeat::run_heartbeat, this,
                                                                        m_stop_source.get_token()} {}

  void run_heartbeat(std::stop_token stoken);
  ~HeartBeat() { m_stop_source.request_stop(); }

  [[nodiscard]] bool IsAlive() const { return m_alive; }

private:
  boost::uuids::uuid m_uuid;
  std::unique_ptr<Connection> m_wsConnection;
  std::stop_source m_stop_source;
  std::jthread m_thread;
  bool m_alive = false;

  Async<void> updateHB(std::stop_token stop_token);
};
} // namespace PMS::Pilot
#endif
