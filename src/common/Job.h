#ifndef PMS_COMMON_JOB_H
#define PMS_COMMON_JOB_H

//#include "common/EnumArray.h"
#include <magic_enum.hpp>

namespace PMS {
// TODO: once we switch to c++17 replace with magic_enum
enum class JobStatus { Pending = 0, Claimed, Running, Done, Error };
constexpr unsigned int nJobStatus = magic_enum::enum_count<JobStatus>();
//static EnumArray<std::string, JobStatus, nJobStatus> JobStatusNames = {"Pending", "Claimed", "Running", "Done",
//                                                                       "Error"};
//static inline JobStatus to_JobStatus(const std::string &name) {
//  auto valueIt = std::find(begin(JobStatusNames), end(JobStatusNames), name);
//  auto value = std::distance(begin(JobStatusNames), valueIt);
//  return static_cast<JobStatus>(value);
//}


} // namespace PMS
#endif