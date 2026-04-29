#pragma once

#include "orchestrator/Server.h"

namespace PMS::Tests::Orchestrator {

// Exposes protected static parsing methods of Server for unit testing,
// without requiring a live Director or websocket setup.
class ServerProxy : public PMS::Orchestrator::Server {
public:
  using Server::toPilotCommand;
  using Server::toUserCommand;
};

} // namespace PMS::Tests::Orchestrator
