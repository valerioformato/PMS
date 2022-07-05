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

namespace PMS::Pilot {

enum class FileTransferType { Inbound, Outbound };
enum class FileTransferProtocol { local, xrootd };

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

  void Process();

  static void ProcessWildcards(std::string &fileName);
  static std::vector<std::string> ExpandWildcard(const FileTransferInfo &ftInfo);

private:
  std::vector<FileTransferInfo> m_queue;

  static std::vector<std::string> GlobFS(std::string_view dir, const std::regex &matcher);
  static std::vector<std::string> GlobXRootD(std::string_view dir, const std::regex &matcher);

  static bool LocalFileTransfer(const FileTransferInfo &ftInfo);
#ifdef ENABLE_XROOTD
  std::vector<XrdCl::PropertyList *> m_results;
  XrdCl::CopyProcess m_xrdProcess;
  static std::vector<std::string> IndexXRootDRemote(std::string_view dir);
  bool AddXRootDFileTransfer(const FileTransferInfo &ftInfo);
  bool RunXRootDFileTransfer();
#endif
};
} // namespace PMS::Pilot

#endif
