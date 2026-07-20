// external headers
#include <boost/uuid/uuid_io.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/fmt/bundled/chrono.h>
#include <spdlog/spdlog.h>

// our headers
#include "pilot/HeartBeat.h"

using json = nlohmann::json;

namespace PMS::Pilot {
void HeartBeat::run_heartbeat() {
  std::stop_token stop_token = m_stop_source.get_token();
  stdexec::sync_wait(updateHB(stop_token));
}

Async<void> HeartBeat::updateHB(std::stop_token stop_token) {
  constexpr static auto coolDown = std::chrono::seconds(15);

  json updateMsg;
  updateMsg["command"] = "p_updateHeartBeat";
  updateMsg["uuid"] = boost::uuids::to_string(m_uuid);

  while (!stop_token.stop_requested()) {
    auto update_result = co_await m_wsConnection->AsyncSend(updateMsg.dump());
    if (update_result && update_result.value() == "Ok") {
      m_consecutive_failures.store(0);
      m_alive.store(true);
    } else {
      auto failures = m_consecutive_failures.fetch_add(1) + 1;
      if (failures >= m_max_consecutive_failures) {
        m_alive.store(false);
      }
    }

    if (!update_result) {
      spdlog::warn("Failed to send heartbeat: {}", update_result.error().Message());
    } else if (update_result.value() != "Ok") {
      spdlog::warn("Unexpected heartbeat reply: {}", update_result.value());
    }

    if (stop_token.stop_requested()) {
      break;
    }

    std::this_thread::sleep_for(coolDown);
  }

  m_alive.store(false);

  json deleteMsg;
  deleteMsg["command"] = "p_deleteHeartBeat";
  deleteMsg["uuid"] = boost::uuids::to_string(m_uuid);
  spdlog::trace("Removing pilot from DB");

  co_await m_wsConnection->AsyncSend(deleteMsg.dump());
}

} // namespace PMS::Pilot
