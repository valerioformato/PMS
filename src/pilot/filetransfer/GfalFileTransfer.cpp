// external dependencies
#include <boost/process.hpp>
#ifdef ENABLE_GFAL2
#include <gfal_api.h>
#endif
#include <spdlog/spdlog.h>

#include "pilot/filetransfer/FileTransferQueue.h"

namespace bp = boost::process;

namespace PMS::Pilot {

#ifdef ENABLE_GFAL2
// Helper functions and types
ErrorOr<gfal2_context_t> CreateGfal2Context() {
  GError *error = nullptr;
  gfal2_context_t context = gfal2_context_new(&error);

  if (error) {
    return Error{static_cast<std::errc>(error->code), error->message};
  }

  return context;
}

struct TransferParameters {
  // default values from gfal2 transfer exmaple
  guint64 timeout{60};
  std::string dst_spacetoken{"TOKEN"};
  bool replace_existing_file{false};
  gfalt_checksum_mode_t checksum_mode{GFALT_CHECKSUM_NONE};
  bool create_parent_directory{true};

  ErrorOr<gfalt_params_t> get_native_parameters() {
    GError *error = nullptr;

    gfalt_params_t params = gfalt_params_handle_new(&error);
    if (error) {
      return Error{static_cast<std::errc>(error->code), error->message};
    }

    gfalt_set_timeout(params, timeout, &error);
    gfalt_set_dst_spacetoken(params, dst_spacetoken.c_str(), &error);
    gfalt_set_replace_existing_file(params, replace_existing_file, &error);
    gfalt_set_checksum(params, checksum_mode, NULL, NULL, NULL);
    gfalt_set_create_parent_dir(params, create_parent_directory, &error);

    return params;
  }
};

ErrorOr<void> Gfal2CopyFile(const std::string &from, const std::string &to, TransferParameters &transfer_parameters,
                            gfal2_context_t context) {
  auto gt_params = TRY(transfer_parameters.get_native_parameters());

  GError *error = nullptr;
  int ret = gfalt_copy_file(context, gt_params, from.c_str(), to.c_str(), &error);

  if (error) {
    return Error{static_cast<std::errc>(error->code), error->message};
  }

  if (ret) {
    return Error{std::errc::io_error, "gfalt_copy_file failed"};
  }

  return outcome::success();
}
#endif

ErrorOr<void> Gfal2CopyFileExternal(const std::string &from, const std::string &to) {
  std::error_code proc_ec;
  int proc_exit_code = 0;
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

  if (proc_ec || proc_exit_code) {
    return Error{proc_ec, fmt::format("gfal-copy failed with exit code {} - {}", proc_exit_code, proc_ec.message())};
  }

  return outcome::success();
}

ErrorOr<void> FileTransferQueue::GfalFileTransfer(const FileTransferInfo &ftInfo) {
  std::string from, to;

  switch (ftInfo.type) {
  case FileTransferType::Inbound:
    from = fmt::format("{}/{}", ftInfo.remotePath, ftInfo.fileName);
    to = fmt::format("file://{}/{}", ftInfo.currentPath, ftInfo.fileName);
    break;
  case FileTransferType::Outbound:
    from = fmt::format("file://{}/{}", ftInfo.currentPath, ftInfo.fileName);
    to = ftInfo.remotePath;
    break;
  }

  spdlog::debug("Attempting to copy {} to {} via gfal", from, to);

#ifndef ENABLE_GFAL2
  // NOTE: in case we cannot find the client library, gfal transfers are implemented by spawning a process
  // call to the actual gfal python client.
  TRY_REPEATED(Gfal2CopyFileExternal(from, to), m_max_tries);

#else
  // First, we need to create a gfal2 context
  gfal2_context_t context = TRY(CreateGfal2Context());

  TransferParameters params{
      .replace_existing_file = true,
      .checksum_mode = GFALT_CHECKSUM_BOTH,
      .create_parent_directory = true,
  };

  // Now, we can start the transfer
  TRY_REPEATED(Gfal2CopyFile(from, to, params, context), m_max_tries);

  // Lastly, we free the context
  gfal2_context_free(context);

#endif

  return outcome::success();
}
} // namespace PMS::Pilot
