#include <spdlog/sinks/stdout_color_sinks.h>

#include "db/harness/Harness.h"

namespace PMS::DB {
std::shared_ptr<spdlog::logger> Harness::m_logger = spdlog::stdout_color_mt("Harness");
} // namespace PMS::DB
