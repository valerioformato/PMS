#ifndef PMS_COMMON_JOB_H
#define PMS_COMMON_JOB_H

#include "common/EnumArray.h"

namespace PMS {
// TODO: once we switch to c++17 replace with magic_enum
constexpr unsigned int nJobStatus = 5;
enum class JobStatus { Pending = 0, Claimed, Running, Done, Error };
static EnumArray<std::string, JobStatus, nJobStatus> JobStatusNames = {"Pending", "Claimed", "Running", "Done",
                                                                       "Error"};
} // namespace PMS
#endif