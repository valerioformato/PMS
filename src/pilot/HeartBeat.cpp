// external headers
#include <boost/uuid/uuid_io.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

// our hreaders
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

  while (exitSignal.wait_for(std::chrono::milliseconds(1)) == std::future_status::timeout) {
    spdlog::trace("Updating HeartBeat");
    auto result = dbHandle.DB()["pilots"].update_one(bsoncxx::from_json(updateFilter.dump()),
                                                     bsoncxx::from_json(updateAction.dump()), updateOpt);
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  spdlog::trace("Removing pilot from DB");
  dbHandle.DB()["pilots"].delete_one(bsoncxx::from_json(updateFilter.dump()));
}

} // namespace Pilot
} // namespace PMS