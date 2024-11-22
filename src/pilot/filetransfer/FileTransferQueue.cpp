// c++ headers
#include <algorithm>
#include <filesystem>

// external dependencies
#include <boost/process.hpp>
#include <regex>
#include <spdlog/spdlog.h>

// our headers
#include "common/Utils.h"
#include "pilot/filetransfer/FileTransferQueue.h"

namespace fs = std::filesystem;
namespace bp = boost::process;

namespace PMS::Pilot {
ErrorOr<void> FileTransferQueue::LocalFileTransfer(const FileTransferInfo &ftInfo) {
  fs::path from, to;

  switch (ftInfo.type) {
  case FileTransferType::Inbound:
    from = fs::path{ftInfo.remotePath} / fs::path{ftInfo.fileName};
    to = fs::path{ftInfo.currentPath};
    break;
  case FileTransferType::Outbound:
    from = fs::path{ftInfo.currentPath} / fs::path{ftInfo.fileName};
    to = fs::path{ftInfo.remotePath};
    if (auto parent_dir = fs::path(to).parent_path(); !fs::exists(parent_dir)) {
      spdlog::warn("Directory {} does not exist. Creating it...", parent_dir.string());
      fs::create_directories(parent_dir);
    }
    break;
  }

  spdlog::debug("Attempting to copy {} to {}", from.string(), to.string());

  try {
    fs::copy(from, to, fs::copy_options::recursive | fs::copy_options::overwrite_existing);
  } catch (const std::exception &e) {
    return Error{std::make_error_code(std::errc::io_error), e.what()};
  }

  return outcome::success();
}

// TODO: gfal transfers are temporarily implemented by spawning a process call to the actual gfal client.
//       With enough time we should use the actual library and embed the file transfer similarly to what we do with
//       xrootd.

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

  std::error_code proc_ec;
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

  std::string out, err;
  while (std::getline(out_stream, out))
    spdlog::trace("stdout: {}", out);

  while (std::getline(err_stream, err))
    spdlog::error("stderr: {}", err);

  if (proc_ec || transfer_process.exit_code()) {
    return Error{std::make_error_code(std::errc::io_error), "gfal-copy failed"};
  }

  return outcome::success();
}

ErrorOr<void> FileTransferQueue::Process() {
  // run the local file transfers
  for (const auto &ft : m_queue) {
    switch (ft.protocol) {
    case FileTransferProtocol::local:
      TRY(LocalFileTransfer(ft));
      break;
    case FileTransferProtocol::gfal:
      TRY(GfalFileTransfer(ft));
      break;
    case FileTransferProtocol::xrootd:
      // handled later
      break;
    }
  };

#ifdef ENABLE_XROOTD
  // run XRootD file transfers
  for (const auto &ft : m_queue) {
    if (ft.protocol == FileTransferProtocol::xrootd)
      TRY(AddXRootDFileTransfer(ft));
  };

  TRY(RunXRootDFileTransfer());
#endif

  return outcome::success();
}

void FileTransferQueue::ProcessWildcards(std::string &fileName) {
  // replace all '.' with escaped dots
  auto dotPos = fileName.find(".");
  while (dotPos != std::string_view::npos) {
    fileName.replace(dotPos, 1, R"(\.)");
    dotPos = fileName.find(".", dotPos + 2);
  }

  auto wildcardPos = fileName.find("*");
  while (wildcardPos != std::string_view::npos) {
    fileName.replace(wildcardPos, 1, R"(.*)");
    wildcardPos = fileName.find("*", wildcardPos + 2);
  }
}

std::vector<std::string> FileTransferQueue::ExpandWildcard(const FileTransferInfo &ftInfo) {
  std::regex matcher{ftInfo.fileName};

  // inbound, xrootd/gfal: glob remote dir
  // inbound, local: glob local dir
  // outbound, *: glob current dir
  std::string_view dir;
  switch (ftInfo.type) {
  case FileTransferType::Outbound:
    dir = ftInfo.currentPath;
    return GlobFS(dir, matcher);
  case FileTransferType::Inbound:
    dir = ftInfo.remotePath;
    switch (ftInfo.protocol) {
    case FileTransferProtocol::local:
      return GlobFS(dir, matcher);
    case FileTransferProtocol::xrootd:
      return GlobXRootD(dir, matcher);
    case FileTransferProtocol::gfal:
      spdlog::warn("Wildcards not yet supported in gfal transfers");
      break;
    }
  }

  return {};
}

std::vector<std::string> FileTransferQueue::GlobFS(std::string_view dir, const std::regex &matcher) {
  std::vector<std::string> result;

  for (const auto &dirEntry : fs::directory_iterator{fs::path{dir}}) {
    std::smatch match;
    std::string fname = dirEntry.path().filename().string();
    if (std::regex_match(fname, match, matcher)) {
      result.push_back(match.str());
    }
  }

  return result;
}

std::vector<std::string> FileTransferQueue::GlobXRootD([[maybe_unused]] std::string_view dir,
                                                       [[maybe_unused]] const std::regex &matcher) {
#ifndef ENABLE_XROOTD
  spdlog::error("XRootD support is not enabled");
  return {};
#else
  std::vector<std::string> result;

  for (const auto &dirEntry : IndexXRootDRemote(dir)) {
    std::smatch match;
    if (std::regex_match(dirEntry, match, matcher)) {
      result.push_back(match.str());
    }
  }

  return result;
#endif
}

} // namespace PMS::Pilot
