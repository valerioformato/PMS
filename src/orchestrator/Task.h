// c++ headers
#include <string>
#include <vector>

namespace PMS {
namespace Orchestrator {
struct Task {
  std::string name;
  unsigned int totJobs;
  unsigned int doneJobs;
  unsigned int failedJobs;
  std::vector<std::string> dependencies;
  bool readyForScheduling = false;

  std::string token;

  bool IsFinished() const { return (totJobs > 0 && doneJobs == totJobs); }
  bool IsActive() const { return (totJobs > 0) && ((doneJobs + failedJobs) != totJobs); }
};
} // namespace Orchestrator
} // namespace PMS