//
// Created by Valerio Formato on 30/07/21.
//

// external dependencies
#include <XrdCl/XrdClConstants.hh>
#include <XrdCl/XrdClFileSystem.hh>
#include <filesystem>
#include <fmt/ranges.h>
#include <magic_enum.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "pilot/Worker.h"

using namespace std::string_view_literals;
namespace fs = std::filesystem;

namespace PMS::Pilot {
enum class XrdProtocol { LOCAL, XROOT, XROOTS, HTTP, HTTPS, OTHER };
const std::map<std::string_view, XrdProtocol> protocolMap = {
    {"xroot"sv, XrdProtocol::XROOT},  {"xroots"sv, XrdProtocol::XROOTS}, {"root"sv, XrdProtocol::XROOT},
    {"roots"sv, XrdProtocol::XROOTS}, {"http"sv, XrdProtocol::HTTP},     {"https"sv, XrdProtocol::HTTPS},
};

static XrdProtocol getProtocol(std::string_view fileName) {
  XrdProtocol protocol = XrdProtocol::LOCAL;
  if (auto pos = fileName.find("://"); pos != std::string_view::npos) {
    if (auto it = protocolMap.find(fileName.substr(0, pos)); it != end(protocolMap)) {
      protocol = it->second;
    } else {
      spdlog::error("Protocol {} not supported", fileName.substr(0, pos));
      protocol = XrdProtocol::OTHER;
    }
  }
  return protocol;
};

static std::vector<std::string> IndexRemote(XrdCl::FileSystem *fs, std::string basePath) {
  XrdCl::DirectoryList *dirList = 0;
  XrdCl::XRootDStatus st =
      fs->DirList(XrdCl::URL{basePath}.GetPath(),
                  XrdCl::DirListFlags::Recursive | XrdCl::DirListFlags::Locate | XrdCl::DirListFlags::Merge, dirList);
  if (!st.IsOK()) {
    spdlog::error("Failed to get directory listing for {}: {}", basePath.c_str(), st.GetErrorMessage().c_str());
    return {};
  }

  std::vector<std::string> result;

  for (auto itr = dirList->Begin(); itr != dirList->End(); ++itr) {
    XrdCl::DirectoryList::ListEntry *e = *itr;
    if (e->GetStatInfo()->TestFlags(XrdCl::StatInfo::IsDir))
      continue;
    std::string path = basePath + '/' + e->GetName();
    result.push_back(path);
  }
  delete dirList;

  return result;
}

static XrdCl::PropertyList getDefaultProperties(std::string_view sfile, std::string_view target, bool targetIsDir) {
  XrdCl::PropertyList properties;
  properties.Set("source", sfile);
  properties.Set("target", target);
  properties.Set("force", true);
  properties.Set("posc", false);
  properties.Set("coerce", false);
  properties.Set("makeDir", true);
  properties.Set("dynamicSource", false);
  properties.Set("thirdParty", "none");
  properties.Set("checkSumMode", "end2end");
  properties.Set("checkSumType", "adler32");
  properties.Set("checkSumPreset", "");
  properties.Set("chunkSize", XrdCl::DefaultCPChunkSize);
  properties.Set("parallelChunks", XrdCl::DefaultCPParallelChunks);
  properties.Set("zipArchive", false);
  properties.Set("xcp", false);
  properties.Set("xcpBlockSize", XrdCl::DefaultXCpBlockSize);
  properties.Set("delegate", false);
  properties.Set("targetIsDir", targetIsDir);
  properties.Set("preserveXAttr", false);
  properties.Set("xrate", 0);
  properties.Set("xrateThreashold", 0);
  properties.Set("rmOnBadCksum", false);
  properties.Set("continue", false);
  properties.Set("zipAppend", false);

  return properties;
}

bool FileTransferQueue::AddXRootDFileTransfer(const FileTransferInfo &ftInfo) {
  std::string sourceFile, destFile;
  switch (ftInfo.type) {
  case FileTransferType::Inbound:
    sourceFile = ftInfo.remotePath + (ftInfo.remotePath.back() == '/' ? "" : "/") + ftInfo.fileName;
    destFile = ftInfo.currentPath + (ftInfo.currentPath.back() == '/' ? "" : "/") + ftInfo.fileName;
    break;
  case FileTransferType::Outbound:
    sourceFile = ftInfo.currentPath + (ftInfo.currentPath.back() == '/' ? "" : "/") + ftInfo.fileName;
    destFile = ftInfo.remotePath + (ftInfo.remotePath.back() == '/' ? "" : "/") + ftInfo.fileName;
    break;
  }

  spdlog::debug("Queueing xrootd copy: {} to {}", sourceFile, destFile);

  auto sourceProtocol = getProtocol(sourceFile);
  // xrootd strips away all trailing slashes except one
  while (sourceFile.rfind("//") == sourceFile.length() - 2) {
    sourceFile.pop_back();
  }

  auto destProtocol = getProtocol(destFile);
  // xrootd strips away all trailing slashes except one
  while (destFile.rfind("//") == destFile.length() - 2) {
    destFile.pop_back();
  }

  // Build the URLs
  std::string dest;
  if (destProtocol == XrdProtocol::LOCAL) {
    dest = "file://";

    // if it is not an absolute path append cwd
    if (destFile.front() != '/') {
      std::error_code ec;
      dest += fs::current_path(ec).string() + '/';
      if (ec) {
        spdlog::error("{}", ec.message());
        return false;
      }
    }
  }
  dest += destFile;

  bool targetIsDir = false;
  auto destPath = fs::path{dest};
  if (fs::is_directory(destPath)) {
    targetIsDir = true;
  } else if (destProtocol == XrdProtocol::XROOT || destProtocol == XrdProtocol::XROOTS) {
    XrdCl::URL target{dest};
    XrdCl::FileSystem fs{target};
    XrdCl::StatInfo *statInfo = nullptr;
    XrdCl::XRootDStatus st = fs.Stat(target.GetPath(), statInfo);
    if (st.IsOK() && statInfo->TestFlags(XrdCl::StatInfo::IsDir)) {
      targetIsDir = true;
    } else if (st.errNo == kXR_NotFound) {
      targetIsDir = (dest.back() == '/');
    }

    delete statInfo;
  }

  bool remoteSrcIsDir = false;
  std::vector<std::string> sourceFiles;
  if (sourceProtocol == XrdProtocol::XROOT) {
    XrdCl::URL source{std::string{sourceFile}};
    auto fs = std::make_unique<XrdCl::FileSystem>(source);
    XrdCl::StatInfo *statInfo = nullptr;

    XrdCl::XRootDStatus st = fs->Stat(source.GetPath(), statInfo);
    if (st.IsOK()) {
      if (statInfo->TestFlags(XrdCl::StatInfo::IsDir)) {
        remoteSrcIsDir = true;
        // Recursively index the remote directory
        std::string url = source.GetURL();
        sourceFiles = IndexRemote(fs.get(), url);
        if (sourceFiles.empty()) {
          spdlog::error("Error indexing remote directory.");
          return false;
        }
      } else {
        sourceFiles.push_back(std::string{sourceFile});
      }
    }

    delete statInfo;
  } else {
    sourceFiles.push_back(std::string{sourceFile});
  }

  for (auto &sfile : sourceFiles) {
    // Create a job for every source
    XrdCl::PropertyList *results = new XrdCl::PropertyList;
    if (sourceProtocol == XrdProtocol::LOCAL) {
      // make sure it is an absolute path
      if (sfile.front() == '/')
        sfile = "file://" + sfile;
      else {
        std::error_code ec;
        sfile = "file://" + fs::current_path(ec).string() + '/' + sfile;
        if (ec) {
          spdlog::error("{}", ec.message());
          return false;
        }
      }
    }

    spdlog::trace("Processing source entry: {}, target file: {}", sfile, dest);

    // Set up the job
    std::string target = dest;

    bool srcIsDir = false;
    auto sourcePath = fs::path(sfile.substr(sfile.find("://") + 3, sfile.length() - 1));
    if (sourceProtocol == XrdProtocol::LOCAL)
      srcIsDir = fs::is_directory(sourcePath);
    else
      srcIsDir = remoteSrcIsDir;
    // if this is a recursive copy make sure we preserve the directory structure
    if (srcIsDir) {
      // get the source directory
      std::string srcDir = sourcePath.parent_path().string();
      // remove the trailing slash
      if (srcDir.back() == '/')
        srcDir.pop_back();
      size_t diroff = srcDir.rfind('/');
      // if there is no '/' it means a directory name has been given as relative path
      if (diroff == std::string::npos)
        diroff = 0;
      target += '/';
      target += sourcePath.filename();
      // remove the filename from destination path as it will be appended later anyway
      target = target.substr(0, target.rfind('/'));
    }

    auto properties = getDefaultProperties(sfile, target, targetIsDir);

    XrdCl::XRootDStatus st = m_xrdProcess.AddJob(properties, results);
    if (!st.IsOK()) {
      spdlog::error("AddJob {} -> {}: {}", sfile, target, st.ToStr());
    }

    m_results.push_back(results);
  }

  return true;
}

bool FileTransferQueue::RunXRootDFileTransfer() {
  // Configure the copy process
  XrdCl::PropertyList processConfig;
  processConfig.Set("jobType", "configuration");
  processConfig.Set("parallel", 1);
  m_xrdProcess.AddJob(processConfig, nullptr);

  auto CleanUpResults = [](auto results) {
    std::for_each(begin(results), end(results), [](auto *result) { delete result; });
  };

  XrdCl::XRootDStatus st = m_xrdProcess.Prepare();
  if (!st.IsOK()) {
    CleanUpResults(m_results);
    spdlog::error("Prepare: {}", st.ToStr());
    return false;
  }

  st = m_xrdProcess.Run(nullptr);
  if (!st.IsOK()) {
    if (m_results.size() == 1)
      spdlog::error("Run: {}", st.ToStr());
    else {
      unsigned int i = 1;
      unsigned int jobsRun = 0;
      unsigned int errors = 0;
      for (auto it = m_results.begin(); it != m_results.end(); ++it, ++i) {
        if (!(*it)->HasProperty("status"))
          continue;

        XrdCl::XRootDStatus st = (*it)->Get<XrdCl::XRootDStatus>("status");
        if (!st.IsOK()) {
          spdlog::error("Job #{}: {}", i, st.ToStr());
          ++errors;
        }
        ++jobsRun;
      }
      spdlog::error("Jobs total: {}, run: {}, errors: {}", m_results.size(), jobsRun, errors);
    }
    CleanUpResults(m_results);
    return false;
  }

  CleanUpResults(m_results);
  return true;
}

std::vector<std::string> FileTransferQueue::IndexXRootDRemote(std::string_view dir) {
  XrdCl::URL source{std::string{dir}};
  auto fs = std::make_unique<XrdCl::FileSystem>(source);
  XrdCl::StatInfo *statInfo = nullptr;
  XrdCl::XRootDStatus st = fs->Stat(source.GetPath(), statInfo);
  std::vector<std::string> result;
  if (st.IsOK() && statInfo->TestFlags(XrdCl::StatInfo::IsDir)) {
    result = IndexRemote(fs.get(), source.GetURL());
  }
  std::for_each(begin(result), end(result), [](std::string &fname) { fname = fs::path{fname}.filename().string(); });

  delete statInfo;

  return result;
}

} // namespace PMS::Pilot