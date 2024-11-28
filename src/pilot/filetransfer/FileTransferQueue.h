#ifndef PMS_PILOT_FILETRANSFERQUEUE_H
#define PMS_PILOT_FILETRANSFERQUEUE_H

// c++ headers
#include <regex>
#include <string>
#include <vector>

// external dependencies
#ifdef ENABLE_XROOTD
#include <XrdCl/XrdClCopyProcess.hh>
#endif

#include "common/Utils.h"

namespace PMS::Pilot {

enum class FileTransferType { Inbound, Outbound };
enum class FileTransferProtocol { local, xrootd, gfal };

struct FileTransferInfo {
  FileTransferType type;
  FileTransferProtocol protocol;
  std::string fileName;
  std::string remotePath;
  std::string currentPath;
};

class FileTransferQueue {
public:
  void Add(const FileTransferInfo &ft) { m_queue.push_back(ft); }
  void Add(FileTransferInfo &&ft) { m_queue.push_back(ft); }

  ErrorOr<void> Process();

  static void ProcessWildcards(std::string &fileName);
  static std::vector<std::string> ExpandWildcard(const FileTransferInfo &ftInfo);

private:
  std::vector<FileTransferInfo> m_queue;

  static constexpr unsigned int m_max_tries = 3;

  static std::vector<std::string> GlobFS(std::string_view dir, const std::regex &matcher);
  static std::vector<std::string> GlobXRootD(std::string_view dir, const std::regex &matcher);

  static ErrorOr<void> LocalFileTransfer(const FileTransferInfo &ftInfo);
#ifdef ENABLE_XROOTD
  std::vector<XrdCl::PropertyList *> m_results;
  XrdCl::CopyProcess m_xrdProcess;
  static std::vector<std::string> IndexXRootDRemote(std::string_view dir);
  ErrorOr<void> AddXRootDFileTransfer(const FileTransferInfo &ftInfo);
  ErrorOr<void> RunXRootDFileTransfer();
#endif
  static ErrorOr<void> GfalFileTransfer(const FileTransferInfo &ftInfo);
};
} // namespace PMS::Pilot

#endif
