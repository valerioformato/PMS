# handle dependencies
include(FetchContent)

# === fmt ===
FetchContent_Declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG 6.1.2)
FetchContent_GetProperties(fmt)
if(NOT fmt_POPULATED)
  set(FMT_INSTALL ON)
  FetchContent_Populate(fmt)
  add_subdirectory(${fmt_SOURCE_DIR} ${fmt_BINARY_DIR})
endif()

# === spdlog ===
# Force spdlog to use downloaded fmt library, using the header-only target
set(SPDLOG_FMT_EXTERNAL_HO
    ON
    CACHE INTERNAL "") # Forces the value
FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG v1.7.0)
FetchContent_GetProperties(spdlog)
if(NOT spdlog_POPULATED)
  set(SPDLOG_INSTALL ON)
  FetchContent_Populate(spdlog)
  add_subdirectory(${spdlog_SOURCE_DIR} ${spdlog_BINARY_DIR})
endif()

# === boost ===
find_package(Boost REQUIRED COMPONENTS filesystem)

# === nlohmannjson ===
FetchContent_Declare(json
  # GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_REPOSITORY https://github.com/ArthurSonzogni/nlohmann_json_cmake_fetchcontent.git
  GIT_TAG v3.7.3)
FetchContent_GetProperties(json)
if(NOT json_POPULATED)
  FetchContent_Populate(json)
  add_subdirectory(${json_SOURCE_DIR} ${json_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

# === docopt ===
FetchContent_Declare(docopt
GIT_REPOSITORY https://github.com/docopt/docopt.cpp
  GIT_TAG v0.6.3)
FetchContent_GetProperties(docopt)
if(NOT docopt_POPULATED)
  FetchContent_Populate(docopt)
  add_subdirectory(${docopt_SOURCE_DIR} ${docopt_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

# === MongoDB C and C++ drivers ===
# 
# FetchContent_Declare(mongodbc
# GIT_REPOSITORY https://github.com/mongodb/mongo-c-driver
#   GIT_TAG 1.17.4)
# FetchContent_GetProperties(mongodbc)
# if(NOT mongodbc_POPULATED)
#   FetchContent_Populate(mongodbc)
#   add_subdirectory(${mongodbc_SOURCE_DIR} ${mongodbc_BINARY_DIR} EXCLUDE_FROM_ALL)
# endif()

# FetchContent_Declare(mongodbcxx
# GIT_REPOSITORY https://github.com/mongodb/mongo-cxx-driver
#   GIT_TAG r3.6.2)
# FetchContent_GetProperties(mongodbcxx)
# if(NOT mongodbcxx_POPULATED)
#   FetchContent_Populate(mongodbcxx)
#   add_subdirectory(${mongodbcxx_SOURCE_DIR} ${mongodbcxx_BINARY_DIR} EXCLUDE_FROM_ALL)
# endif()

find_package(bsoncxx REQUIRED)
find_package(mongocxx REQUIRED 3.6.0)

