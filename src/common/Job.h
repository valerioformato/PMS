#include "common/EnumArray.h"

namespace PMS {
// TODO: once we switch to c++17 replace with magic_enum
constexpr unsigned int nJobStatus = 4;
enum class JobStatus { Pending, Running, Done, Error };
static EnumArray<std::string, JobStatus, nJobStatus> JobStatusNames = {"Pending", "Running", "Done", "Error"};
} // namespace PMS