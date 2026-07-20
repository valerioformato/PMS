#ifndef PMS_PILOT_HEARTBEAT_H
#define PMS_PILOT_HEARTBEAT_H

// c++ headers
#include <atomic>
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
      : m_uuid{uuid}, m_wsConnection{std::move(wsConnection)}, m_thread{&HeartBeat::run_heartbeat, this} {}

  void run_heartbeat();
  ~HeartBeat() {
    m_stop_source.request_stop();
    if (m_thread.joinable()) {
      m_thread.join();
    }
  }

  [[nodiscard]] bool IsAlive() const { return m_alive.load(); }

private:
  boost::uuids::uuid m_uuid;
  std::unique_ptr<Connection> m_wsConnection;
  std::stop_source m_stop_source;
  std::thread m_thread;
  std::atomic_bool m_alive{true};
  std::atomic_uint m_consecutive_failures{0};
  static constexpr unsigned int m_max_consecutive_failures = 3;

  Async<void> updateHB(std::stop_token stop_token);
};
} // namespace PMS::Pilot
#endif
