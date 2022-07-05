#ifndef PMS_COMMON_JOB_H
#define PMS_COMMON_JOB_H

#include <magic_enum.hpp>

namespace PMS {
// TODO: once we switch to c++17 replace with magic_enum
enum class JobStatus { Pending = 0, Claimed, Running, Done, Error, Failed };
constexpr unsigned int nJobStatus = magic_enum::enum_count<JobStatus>();

} // namespace PMS
#endif
