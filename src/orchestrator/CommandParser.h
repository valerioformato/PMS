#pragma once

#include "orchestrator/Commands.h"

namespace PMS::Orchestrator::CommandParser {

UserCommand toUserCommand(const json &msg);
PilotCommand toPilotCommand(const json &msg);

} // namespace PMS::Orchestrator::CommandParser
