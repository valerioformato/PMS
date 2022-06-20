// c++ headers
#include <algorithm>
#include <filesystem>

// external dependencies
#include <regex>
#include <spdlog/spdlog.h>

// our headers
#include "pilot/filetransfer/FileTransferQueue.h"

namespace fs = std::filesystem;

namespace PMS::Pilot {
bool FileTransferQueue::LocalFileTransfer(const FileTransferInfo &ftInfo) {
  fs::path from, to;

  switch (ftInfo.type) {
  case FileTransferType::Inbound:
    from = fs::path{ftInfo.remotePath} / fs::path{ftInfo.fileName};
    to = fs::path{ftInfo.currentPath};
    break;
  case FileTransferType::Outbound:
    from = fs::path{ftInfo.currentPath} / fs::path{ftInfo.fileName};
    to = fs::path{ftInfo.remotePath};
    if (!fs::exists(to)){
      spdlog::warn("Directory {} does not exist. Creating it...", to.string());
      fs::create_directories(to);
    }
    break;
  }

  spdlog::debug("Attempting to copy {} to {}", from.string(), to.string());

  try {
    fs::copy(from, to, fs::copy_options::recursive | fs::copy_options::overwrite_existing);
  } catch (const fs::filesystem_error &e) {
    spdlog::error("{}", e.what());
    return false;
  }

  return true;
}

void FileTransferQueue::Process() {
  // run the local file transfers
  std::for_each(begin(m_queue), end(m_queue), [](const auto &ft) {
    if (ft.protocol == FileTransferProtocol::local)
      LocalFileTransfer(ft);
  });

#ifdef ENABLE_XROOTD
  // run XRootD file transfers
  std::for_each(begin(m_queue), end(m_queue), [this](const auto &ft) {
    if (ft.protocol == FileTransferProtocol::xrootd)
      AddXRootDFileTransfer(ft);
  });
  auto result = RunXRootDFileTransfer();
  if (!result) {
    spdlog::error("XRootD transfer failed, check previous messages");
  }
#endif
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

  // inbound, xrootd: glob remote dir
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