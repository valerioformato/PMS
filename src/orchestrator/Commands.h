//
// Created by Valerio Formato on 20/07/21.
//

#ifndef PMS_COMMANDS_H
#define PMS_COMMANDS_H

#include <nlohmann/json.hpp>

#include "orchestrator/PilotCommands.h"
#include "orchestrator/UserCommands.h"

using json = nlohmann::json;

namespace PMS::Orchestrator {
template <typename Command> struct OrchCommand { Command cmd; };

struct InvalidCommand {
  std::string errorMessage;
};

using UserCommand = std::variant<OrchCommand<InvalidCommand>, OrchCommand<SubmitJob>, OrchCommand<FindJobs>,
                                 OrchCommand<FindPilots>, OrchCommand<ResetJobs>, OrchCommand<CreateTask>,
                                 OrchCommand<ClearTask>, OrchCommand<CleanTask>, OrchCommand<DeclareTaskDependency>,
                                 OrchCommand<CheckTaskToken>, OrchCommand<Summary>, OrchCommand<ResetFailedJobs>>;

using PilotCommand = std::variant<OrchCommand<InvalidCommand>, OrchCommand<ClaimJob>, OrchCommand<UpdateJobStatus>,
                                  OrchCommand<RegisterNewPilot>, OrchCommand<UpdateHeartBeat>,
                                  OrchCommand<DeleteHeartBeat>, OrchCommand<Test>>;

template <typename Command> static inline bool ValidateJsonCommand(const json &msg) {
  return std::accumulate(begin(Command::requiredFields), end(Command::requiredFields), true,
                         [&msg](bool currValue, auto field) { return currValue && msg.contains(field); });
}
} // namespace PMS::Orchestrator
#endif // PMS_COMMANDS_H
