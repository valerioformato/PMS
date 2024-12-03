// external dependencies
#include <boost/process.hpp>
#include <gfal_api.h>
#include <spdlog/spdlog.h>

#include "pilot/filetransfer/FileTransferQueue.h"

namespace bp = boost::process;

namespace PMS::Pilot {

ErrorOr<void> FileTransferQueue::GfalFileTransfer(const FileTransferInfo &ftInfo) {
  std::string from, to;

  switch (ftInfo.type) {
  case FileTransferType::Inbound:
    from = fmt::format("{}/{}", ftInfo.remotePath, ftInfo.fileName);
    to = ftInfo.currentPath;
    break;
  case FileTransferType::Outbound:
    from = fmt::format("{}/{}", ftInfo.currentPath, ftInfo.fileName);
    to = ftInfo.remotePath;
    break;
  }

  spdlog::debug("Attempting to copy {} to {} via gfal", from, to);

  unsigned int tries = 0;

#ifndef ENABLE_GFAL2
  // NOTE: in case we cannot find the client library, gfal transfers are implemented by spawning a process
  // call to the actual gfal python client.

  std::error_code proc_ec;
  int proc_exit_code = 0;
  while (tries < m_max_tries) {
    bp::ipstream out_stream, err_stream;
    bp::child transfer_process{bp::search_path("gfal-copy"),
                               std::string{"-p"},
                               std::string{"-f"},
                               from,
                               to,
                               bp::std_out > out_stream,
                               bp::std_err > err_stream,
                               proc_ec};
    transfer_process.wait();

    proc_exit_code = transfer_process.exit_code();

    std::string out, err;
    while (std::getline(out_stream, out))
      spdlog::trace("stdout: {}", out);

    while (std::getline(err_stream, err))
      spdlog::error("stderr: {}", err);

    if (!proc_ec && !proc_exit_code) {
      break;
    }

    ++tries;
  }

  if (proc_ec || proc_exit_code) {
    return Error{proc_ec, fmt::format("gfal-copy failed with exit code {} - {}", proc_exit_code, proc_ec.message())};
  }
#else
#endif

  return outcome::success();
}
} // namespace PMS::Pilot
