// c++ headers
#include <string>
#include <vector>

namespace PMS::Orchestrator {
struct Task {
  std::string name;
  std::string owner;
  unsigned int totJobs;
  unsigned int doneJobs;
  unsigned int failedJobs;
  std::vector<std::string> dependencies;
  bool readyForScheduling = false;

  std::string token;

  [[nodiscard]] bool IsFinished() const { return (totJobs > 0 && doneJobs == totJobs); }
  [[nodiscard]] bool IsActive() const { return (totJobs > 0) && ((doneJobs + failedJobs) != totJobs); }
};
} // namespace PMS::Orchestrator