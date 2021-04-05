// external headers
#include <spdlog/spdlog.h>

// our hreaders
#include "pilot/HeartBeat.h"

namespace PMS {
namespace Pilot {
HeartBeat::~HeartBeat() {
  spdlog::debug("Stopping HeartBeat");
  m_exitSignal.set_value();
  m_thread.join();
}

void HeartBeat::updateHB(std::future<void> exitSignal) {
  spdlog::debug("Starting HeartBeat");
  while (exitSignal.wait_for(std::chrono::milliseconds(1)) == std::future_status::timeout) {
    spdlog::trace("Updating HeartBeat");
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

} // namespace Pilot
} // namespace PMS