// c++ headers
#include <algorithm>
#include <filesystem>

// external dependencies
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
    to = fs::path{ftInfo.currentPath} / fs::path{ftInfo.fileName};
    break;
  case FileTransferType::Outbound:
    from = fs::path{ftInfo.currentPath} / fs::path{ftInfo.fileName};
    to = fs::path{ftInfo.remotePath} / fs::path{ftInfo.fileName};
    break;
  }

  try {
    fs::copy(from, to, fs::copy_options::recursive | fs::copy_options::overwrite_existing);
  } catch (const fs::filesystem_error &e) {
    spdlog::error("{}", e.what());
    return false;
  }

  return true;
}

void FileTransferQueue::Process() {
  // put all local file transfers first
  auto lastLocalJobIt = std::partition(begin(m_queue), end(m_queue),
                                       [](const auto &ft) { return ft.protocol == FileTransferProtocol::local; });

  // run the local file transfers
  std::for_each(begin(m_queue), lastLocalJobIt, [](const auto &ft) { LocalFileTransfer(ft); });

#ifdef ENABLE_XROOTD
  // run XRootD file transfers
  std::for_each(lastLocalJobIt, end(m_queue), [this](const auto &ft) { AddXRootDFileTransfer(ft); });
  RunXRootDFileTransfer();
#endif
}

} // namespace PMS::Pilot