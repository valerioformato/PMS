#ifndef PMS_COMMON_JOB_H
#define PMS_COMMON_JOB_H

#include <magic_enum.hpp>

namespace PMS {

enum class JobStatus { Pending = 0, Claimed, Running, Done, Error, Failed, InboundTransfer, OutboundTransfer };
constexpr unsigned int nJobStatus = magic_enum::enum_count<JobStatus>();

} // namespace PMS
#endif
