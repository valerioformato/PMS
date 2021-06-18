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

  json updateFilter;
  updateFilter["uuid"] = boost::uuids::to_string(m_uuid);

  json updateAction;
  updateAction["$currentDate"]["lastHeartBeat"] = true;

  mongocxx::options::update updateOpt{};
  updateOpt.upsert(true);

  spdlog::trace("{}, {}", updateFilter.dump(), updateAction.dump());

  DB::DBHandle dbHandle = m_poolHandle->DBHandle();

  do {
    spdlog::trace("Updating HeartBeat");
    dbHandle["pilots"].update_one(JsonUtils::json2bson(updateFilter), JsonUtils::json2bson(updateAction), updateOpt);
  } while (exitSignal.wait_for(std::chrono::seconds(1)) == std::future_status::timeout);

  spdlog::trace("Removing pilot from DB");
  dbHandle["pilots"].delete_one(JsonUtils::json2bson(updateFilter));
}

} // namespace Pilot
} // namespace PMS