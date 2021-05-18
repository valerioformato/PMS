#include "orchestrator/Director.h"

namespace PMS {
namespace Orchestrator {
void Director::Start() { m_backPoolHandle->DBHandle().SetupJobIndexes(); }
} // namespace Orchestrator
} // namespace PMS