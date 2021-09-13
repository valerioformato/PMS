// external headers
#include <boost/uuid/uuid_io.hpp>
#include <fmt/chrono.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "pilot/HeartBeat.h"

using json = nlohmann::json;

namespace PMS::Pilot {
HeartBeat::~HeartBeat() {
  spdlog::debug("Stopping HeartBeat");
  m_exitSignal.set_value();
  m_thread.join();
}

void HeartBeat::updateHB(std::future<void> exitSignal) {
  constexpr static auto coolDown = std::chrono::seconds(15);

  spdlog::debug("Starting HeartBeat");
  m_alive = true;

  constexpr static auto gracePeriod = std::chrono::hours(1);

  json updateMsg;
  updateMsg["command"] = "p_updateHeartBeat";
  updateMsg["uuid"] = boost::uuids::to_string(m_uuid);

  auto connection = m_wsClient->PersistentConnection();
  std::chrono::system_clock::time_point firstFailedConnection;
  bool failedToConnect;

  do {
    spdlog::trace("Updating HeartBeat");
    try {
      connection->Send(updateMsg.dump());
      failedToConnect = false;
    } catch (...) {
      if (!failedToConnect) {
        failedToConnect = true;
        firstFailedConnection = std::chrono::system_clock::now();
      }

      if (std::chrono::system_clock::now() - firstFailedConnection > gracePeriod) {
        spdlog::error("Could not update heartbeat for {}. Shutting down...", gracePeriod);
        m_alive = false;
      }
    }

  } while (exitSignal.wait_for(coolDown) == std::future_status::timeout);

  json deleteMsg;
  deleteMsg["command"] = "p_deleteHeartBeat";
  deleteMsg["uuid"] = boost::uuids::to_string(m_uuid);
  spdlog::trace("Removing pilot from DB");
  try {
    connection->Send(deleteMsg.dump());
  } catch (...) {
  }
}

} // namespace PMS::Pilot