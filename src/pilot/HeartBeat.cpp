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

auto unwrap(std::exception_ptr ptr) -> const std::exception & {
  try {
    std::rethrow_exception(ptr);
  } catch (std::exception &e) {
    return e;
  }
}

void HeartBeat::updateHB(std::future<void> exitSignal) {
  constexpr static auto coolDown = std::chrono::seconds(15);

  spdlog::debug("Starting HeartBeat");
  m_alive = true;

  constexpr static auto gracePeriod = std::chrono::hours(1);

  json updateMsg;
  updateMsg["command"] = "p_updateHeartBeat";
  updateMsg["uuid"] = boost::uuids::to_string(m_uuid);

  std::chrono::system_clock::time_point firstFailedConnection;
  bool failedToConnect = false;

  do {
    // spdlog::trace("Updating HeartBeat");
    try {
      m_wsConnection->Send(updateMsg.dump());
      failedToConnect = false;
    } catch (const Connection::FailedConnectionException &e) {
      spdlog::warn("Exception thrown while sending heartbeat: {}", e.what());
      if (!failedToConnect) {
        failedToConnect = true;
        firstFailedConnection = std::chrono::system_clock::now();
      }

      if (std::chrono::system_clock::now() - firstFailedConnection > gracePeriod) {
        spdlog::error("Could not update heartbeat for {}. Shutting down...", gracePeriod);
        m_alive = false;
        m_exitSignal.set_value();
      }
    } catch (...) {
      const auto &e = unwrap(std::current_exception());
      spdlog::error("Unexpected exception: '{}'", e.what());
    }

  } while (exitSignal.wait_for(coolDown) == std::future_status::timeout);

  json deleteMsg;
  deleteMsg["command"] = "p_deleteHeartBeat";
  deleteMsg["uuid"] = boost::uuids::to_string(m_uuid);
  spdlog::trace("Removing pilot from DB");
  try {
    m_wsConnection->Send(deleteMsg.dump());
  } catch (...) {
  }
}

} // namespace PMS::Pilot
