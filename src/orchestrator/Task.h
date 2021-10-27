// c++ headers
#include <string>
#include <vector>

// our headers
#include "common/EnumArray.h"
#include "common/Job.h"

namespace PMS::Orchestrator {
struct Task {
  std::string name;
  std::string owner;
  unsigned int totJobs;
  EnumArray<unsigned int, JobStatus> jobs;
  std::vector<std::string> dependencies;
  bool readyForScheduling = false;

  std::string token;

  [[nodiscard]] bool IsFinished() const { return (totJobs > 0 && jobs[JobStatus::Done] == totJobs); }
  [[nodiscard]] bool IsActive() const {
    return (totJobs > 0) && ((jobs[JobStatus::Done] + jobs[JobStatus::Failed]) != totJobs);
  }
  [[nodiscard]] bool IsExhausted() const { return (totJobs > 0 && jobs[JobStatus::Pending] == 0); }
};
} // namespace PMS::Orchestrator