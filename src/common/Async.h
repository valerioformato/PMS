#ifndef PMS_COMMON_ASYNC_H
#define PMS_COMMON_ASYNC_H

#include <stdexec/execution.hpp>

namespace PMS {

template <typename T> using Async = stdexec::task<T>;

} // namespace PMS

#endif
