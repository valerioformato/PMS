#include "orchestrator/Director.h"

namespace PMS {
namespace Orchestrator {
void Director::Start() { m_backPoolHandle->DBHandle().SetupJobIndexes(); }
void Director::Stop() {}
void Director::UpdateTasks() {}
} // namespace Orchestrator
} // namespace PMS