#ifndef PMS_PILOT_FILETRANSFERQUEUE_H
#define PMS_PILOT_FILETRANSFERQUEUE_H

// c++ headers
#include <string>
#include <vector>

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

private:
  std::vector<FileTransferInfo> m_queue;

  static bool LocalFileTransfer(const FileTransferInfo &ftInfo);
  static bool XRootDFileTransfer(const FileTransferInfo &ftInfo);
};
} // namespace PMS::Pilot

#endif