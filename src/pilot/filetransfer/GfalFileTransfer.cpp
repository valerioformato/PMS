// external dependencies
#include "pilot/filetransfer/FileTransferQueue.h"

#include <boost/process.hpp>
#ifdef ENABLE_GFAL2
#include <gfal_api.h>
#endif
#include <fmt/chrono.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <filesystem>
#include <ranges>

namespace bp = boost::process;

namespace PMS::Pilot {
ErrorOr<bool> IsDirectory(const std::string_view &path, bool is_remote);

std::vector<std::string> FlattenDirectory(const std::filesystem::path &path);

std::vector<std::string> FlattenRemoteDirectory(std::string_view remote_path,
                                                std::optional<gfal2_context_t> o_context = std::nullopt);

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
  // default values from gfal2 transfer example
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
  static auto logger = spdlog::stdout_color_st("gfal2-monitor-callback");

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
  static auto logger = spdlog::stdout_color_mt("GfalFileTransfer");

  std::string from, to;

  switch (ftInfo.type) {
  case FileTransferType::Inbound:
    from = fmt::format("{}/{}", ftInfo.remotePath, ftInfo.fileName);
    to = fmt::format("file:///{}/{}", ftInfo.currentPath, ftInfo.fileName);
    break;
  case FileTransferType::Outbound:
    from = fmt::format("file:///{}/{}", ftInfo.currentPath, ftInfo.fileName);
    to = ftInfo.remotePath;
    break;
  }

  logger->debug("Attempting to copy {} to {} via gfal", from, to);

#ifndef ENABLE_GFAL2
  // NOTE: in case we cannot find the client library, gfal transfers are implemented by spawning a process
  // call to the actual gfal python client.
  TRY_REPEATED(Gfal2CopyFileExternal(from, to), m_max_tries);

#else
  // First, we need to create a gfal2 context
  static gfal2_context_t context = TRY(CreateGfal2Context());

  auto local_path = fmt::format("{}/{}", ftInfo.currentPath, ftInfo.fileName);
  if (ftInfo.type == FileTransferType::Outbound && IsDirectory(local_path, false).value()) {
    logger->warn("Directory transfer requested, this is still experimental");

    auto ft_info_list =
        std::views::all(FlattenDirectory(local_path)) | std::views::transform([&](const std::string &file) {
          return FileTransferInfo{
              .type = FileTransferType::Outbound,
              .protocol = ftInfo.protocol,
              .fileName = file,
              .remotePath = fmt::format("{}/{}", to, file),
              .currentPath = from,
          };
        });

    for (const auto &ft_info : ft_info_list) {
      TRY(GfalFileTransfer(ft_info));
    }
  } else {
    TransferParameters params{
        .replace_existing_file = true,
        .checksum_mode = GFALT_CHECKSUM_TARGET,
        .create_parent_directory = true,
    };

    // Now, we can start the transfer
    TRY_REPEATED(Gfal2CopyFile(from, to, params, context), m_max_tries);
  }

  // Lastly, we free the context
  gfal2_context_free(context);
#endif

  return outcome::success();
}

std::vector<std::string> FlattenDirectory(const std::filesystem::path &path) {
  static auto logger = spdlog::stdout_color_mt("FlattenDirectory");

  std::vector<std::string> files;

  try {
    if (!std::filesystem::exists(path) || !std::filesystem::is_directory(path)) {
      logger->error("Path {} does not exist or is not a directory", path.string());
      return {};
    }

    // Get the canonical path to ensure consistent path comparisons
    const auto canonical_path = std::filesystem::canonical(path);

    // Recursively iterate through the directory
    for (const auto &entry : std::filesystem::recursive_directory_iterator(
             canonical_path, std::filesystem::directory_options::follow_directory_symlink)) {
      // Only include regular files, not directories
      if (std::filesystem::is_regular_file(entry)) {
        // Calculate the relative path from the base directory
        std::filesystem::path relative_path = std::filesystem::relative(entry, canonical_path);
        logger->debug("Adding {} to the filelist", relative_path.string());
        files.push_back(relative_path.string());
      }
    }

    logger->debug("Found {} files in directory {}", files.size(), path.string());
  } catch (const std::filesystem::filesystem_error &e) {
    logger->error("Filesystem error while flattening directory {}: {}", path.string(), e.what());
  } catch (const std::exception &e) {
    logger->error("Error while flattening directory {}: {}", path.string(), e.what());
  }

  return files;
}

std::vector<std::string> FlattenRemoteDirectory(const std::string_view remote_path,
                                                std::optional<gfal2_context_t> o_context) {
  std::vector<std::string> files;

  static gfal2_context_t context;

  // First, we need to create a gfal2 context
  if (!o_context) {
    auto maybe_context = CreateGfal2Context();
    if (maybe_context.has_error()) {
      spdlog::error("Failed to create gfal2 context: {}", maybe_context.error().Message());
      return {};
    }
    context = maybe_context.value();
  } else {
    context = o_context.value();
  }

  static bool timeout_setup{false};
  if (!timeout_setup) {
    gfal2_set_opt_integer(context, "CORE", "NAMESPACE_TIMEOUT", 1000000, nullptr);
    int timeout = gfal2_get_opt_integer(context, "CORE", "NAMESPACE_TIMEOUT", nullptr);
    if (timeout != 1000000) {
      spdlog::error("Timeout not set. Got value of {}", timeout);
      return {};
    } else {
      timeout_setup = true;
    }
  }

  if (auto check_result = IsDirectory(remote_path, true); check_result.has_value() && check_result.value()) {
    spdlog::trace("Recursing into {}", remote_path);

    // If the path is a directory, we can list its contents
    GError *error = nullptr;
    DIR *dir = gfal2_opendir(context, remote_path.data(), &error);
    dirent *dir_entry = gfal2_readdir(context, dir, &error);
    while (dir_entry || !error) {
      if (error) {
        spdlog::error("Failed to read directory {}: {}", remote_path, error->message);
        spdlog::error("dir_entry was: {} {}", fmt::ptr(dir_entry), remote_path);
        g_error_free(error);
        return files;
      }

      std::string_view entry_name(dir_entry->d_name);
      if (IsDirectory(fmt::format("{}/{}", remote_path, entry_name), true)) {
        auto sub_files = FlattenRemoteDirectory(fmt::format("{}/{}", remote_path, entry_name));

        files.reserve(files.size() + sub_files.size() + 1);
        // Reserve space for the sub-files and the current directory entry
        std::ranges::copy(sub_files, std::back_inserter(files));
      }

      dir_entry = gfal2_readdir(context, dir, &error);
      spdlog::trace("Current: {} - Next dir_entry: {} {} ({} {})", entry_name, fmt::ptr(dir_entry),
                    dir_entry ? dir_entry->d_name : "", fmt::ptr(error), error ? error->message : "");
    }

    if (error) {
      g_error_free(error);
    }

    gfal2_closedir(context, dir, &error);

    spdlog::trace("FlattenDirectory on {} returned {} files", remote_path, files.size());
  } else {
    // spdlog::trace("Adding {}", remote_path);
    files.emplace_back(remote_path);
  }

  return files;
}

ErrorOr<bool> IsDirectory(const std::string_view &path, bool is_remote) {
  static auto logger = spdlog::stdout_color_st("IsDirectory");

  // Helper function to check if a path is a directory
  if (!is_remote) {
    try {
      logger->trace("{} is {} a directory", path, std::filesystem::is_directory(path) ? "" : "not");
      return std::filesystem::is_directory(path);
    } catch (const std::filesystem::filesystem_error &e) {
      logger->error("Error checking if path is a directory: {}", e.what());
      return Error(std::make_error_code(std::errc::io_error), e.what());
    }
  } else {
    // For remote paths, we need to use gfal2_stat
    static auto context = TRY(CreateGfal2Context());

    GError *error = nullptr;

    struct stat statbuf;
    int ret = gfal2_stat(context, path.data(), &statbuf, &error);
    logger->trace("{} is {} a directory", path, S_ISDIR(statbuf.st_mode) ? "" : "not");

    if (ret == -1) {
      logger->error("Failed to stat remote path {}: {}", path, error->message);
      return Error(std::make_error_code(std::errc::io_error), error->message);
    }

    if (error) {
      g_error_free(error);
    }

    return S_ISDIR(statbuf.st_mode);
  }
}
} // namespace PMS::Pilot
