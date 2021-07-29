//
// Created by Valerio Formato on 28/07/21.
//

// external dependencies
#include <filesystem>
#include <magic_enum.hpp>
#include <spdlog/spdlog.h>

// our headers
#include "pilot/Worker.h"

using namespace std::string_view_literals;
namespace fs = std::filesystem;

namespace PMS::Pilot {

bool Worker::LocalFileTransfer(const FileTransferInfo &ftInfo) {
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

bool Worker::FileTransfer(const FileTransferInfo &ftInfo) {
  switch (ftInfo.protocol) {
  case FileTransferProtocol::local:
    spdlog::debug("Will attempt local file transfer");
    LocalFileTransfer(ftInfo);
    break;
  case FileTransferProtocol::xrootd:
    spdlog::debug("Will attempt XRootD file transfer");
    break;
  }

  return true;
}

std::vector<Worker::FileTransferInfo> Worker::ParseFileTransferRequest(FileTransferType type, const json &request, std::string_view currentPath) {
  std::vector<Worker::FileTransferInfo> result;

  if (!request.contains("files")) {
    spdlog::error(R"(No "files" field present in file transfer request.)");
    return result;
  }

  constexpr static std::array requiredFields{"file"sv, "protocol"sv};
  std::vector<std::string_view> additionalFields;
  switch (type) {
  case FileTransferType::Inbound:
    additionalFields = {"source"};
    break;
  case FileTransferType::Outbound:
    additionalFields = {"destination"};
    break;
  }

  for (const auto &doc : request["files"]) {
    for (const auto field : requiredFields) {
      if (!doc.contains(field)) {
        spdlog::error("Missing file transfer field: \"{}\"", field);
        continue;
      }
    }

    for (const auto field : additionalFields) {
      if (!doc.contains(field)) {
        spdlog::error("Missing file transfer field: \"{}\"", field);
        continue;
      }
    }

    auto protocol = magic_enum::enum_cast<FileTransferProtocol>(doc["protocol"].get<std::string_view>());
    if (!protocol.has_value()) {
      spdlog::error("Invalid file transfer protocol: {}", doc["protocol"]);
    }

    // cannot rely on emplace_back + aggregate initialization here until c++20
    result.push_back(FileTransferInfo{type, protocol.value(), doc["file"],
                                      type == FileTransferType::Inbound ? doc["source"] : doc["destination"],
                                      std::string{currentPath}});
  }

  return result;
}

} // namespace PMS::Pilot