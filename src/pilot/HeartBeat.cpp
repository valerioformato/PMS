// external headers
#include <boost/uuid/uuid_io.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "common/JsonUtils.h"
#include "pilot/HeartBeat.h"

using json = nlohmann::json;

namespace PMS {
namespace Pilot {
HeartBeat::~HeartBeat() {
  spdlog::debug("Stopping HeartBeat");
  m_exitSignal.set_value();
  m_thread.join();
}

void HeartBeat::updateHB(std::future<void> exitSignal) {
  spdlog::debug("Starting HeartBeat");

  json updateMsg;
  updateMsg["command"] = "p_updateHeartBeat";
  updateMsg["uuid"] = boost::uuids::to_string(m_uuid);

  auto connection = m_wsClient->PersistentConnection();

  do {
    spdlog::trace("Updating HeartBeat");
    try {
      connection->Send(updateMsg.dump());
    } catch (const std::exception &e) {
      spdlog::error("{}", e.what());
    }

  } while (exitSignal.wait_for(std::chrono::seconds(1)) == std::future_status::timeout);

  json deleteMsg;
  deleteMsg["command"] = "p_deleteHeartBeat";
  deleteMsg["uuid"] = boost::uuids::to_string(m_uuid);
  spdlog::trace("Removing pilot from DB");
  try {
    connection->Send(deleteMsg.dump());
  } catch (const std::exception &e) {
    spdlog::error("{}", e.what());
  }
}

} // namespace Pilot
} // namespace PMS