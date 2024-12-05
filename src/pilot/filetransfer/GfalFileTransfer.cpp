// external dependencies
#include <boost/process.hpp>
#ifdef ENABLE_GFAL2
#include <gfal_api.h>
#endif
#include <fmt/chrono.h>
#include <spdlog/sinks/stdout_color_sinks.h>
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
  guint64 timeout{120};
  std::string dst_spacetoken{"TOKEN"};
  bool replace_existing_file{false};
  gfalt_checksum_mode_t checksum_mode{GFALT_CHECKSUM_NONE};
  bool create_parent_directory{true};

  ErrorOr<gfalt_params_t> get_native_parameters() const {
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

static void EventCallback(const gfalt_event_t e, [[maybe_unused]] gpointer user_data) {
  static auto logger = spdlog::stdout_color_st("gfal2-event-callback");

  std::string side_str;
  switch (e->side) {
  case GFAL_EVENT_SOURCE:
    side_str = "SOURCE";
    break;
  case GFAL_EVENT_DESTINATION:
    side_str = "DESTINATION";
    break;
  default:
    side_str = "BOTH";
  }

  std::string domain = g_quark_to_string(e->domain);
  std::string stage = g_quark_to_string(e->stage);

  logger->trace("Event received: {} {} {} {}", side_str, domain, stage, e->description);
}

static void MonitorCallback(gfalt_transfer_status_t h, [[maybe_unused]] const char *src,
                            [[maybe_unused]] const char *dst, [[maybe_unused]] gpointer user_data) {
  static auto logger = spdlog::stdout_color_st("gfal2-event-callback");

  if (!h) {
    return;
  }

  auto format_converted_bytes = [](size_t bytes) {
    if (bytes < 1024) {
      return fmt::format("{} B", bytes);
    } else if (bytes < 1024 * 1024) {
      return fmt::format("{:.2f} KB", bytes / 1024.0);
    } else if (bytes < 1024 * 1024 * 1024) {
      return fmt::format("{:.2f} MB", bytes / (1024.0 * 1024));
    } else {
      return fmt::format("{:.2f} GB", bytes / (1024.0 * 1024 * 1024));
    }
  };

  size_t avg = gfalt_copy_get_average_baudrate(h, nullptr);
  size_t inst = gfalt_copy_get_instant_baudrate(h, nullptr);
  size_t trans = gfalt_copy_get_bytes_transferred(h, nullptr);
  auto elapsed = std::chrono::seconds{gfalt_copy_get_elapsed_time(h, nullptr)};

  logger->debug("{} /second average ({} instant). Transferred {}, elapsed {} seconds", format_converted_bytes(avg),
                format_converted_bytes(inst), format_converted_bytes(trans), elapsed);
}

ErrorOr<void> Gfal2CopyFile(const std::string &from, const std::string &to,
                            const TransferParameters &transfer_parameters, gfal2_context_t context) {
  static auto logger = spdlog::stdout_color_st("gfal2-copy-file");

  auto gt_params = TRY(transfer_parameters.get_native_parameters());

  GError *error = nullptr;

  gfalt_add_event_callback(gt_params, EventCallback, nullptr, nullptr, &error); // Called when some event is triggered
  gfalt_add_monitor_callback(gt_params, MonitorCallback, nullptr, nullptr, &error); // Performance monitor

  int ret = -1;
  // repeat copy skipping all instances where the http error code is 500 with a MAKE_PARENT cause
  do {
    ret = gfalt_copy_file(context, gt_params, from.c_str(), to.c_str(), &error);
    if (error && error->message) {
      logger->error("failed: {}", error->message);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  } while (error && error->message && strstr(error->message, "HTTP 500") && strstr(error->message, "MAKE_PARENT"));

  if (error) {
    gfalt_params_handle_delete(gt_params, &error);
    return Error{static_cast<std::errc>(error->code), error->message};
  }

  if (ret) {
    gfalt_params_handle_delete(gt_params, &error);
    return Error{std::errc::io_error, "gfalt_copy_file failed"};
  }

  gfalt_params_handle_delete(gt_params, &error);
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
      .checksum_mode = GFALT_CHECKSUM_TARGET,
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
