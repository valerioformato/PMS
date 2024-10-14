# handle dependencies
include(FetchContent)

# needed for the fmt and spdlog static libraries
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# https://github.com/Neargye/magic_enum.git
# === magic_enum ===
FetchContent_Declare(
        magic_enum
        GIT_REPOSITORY https://github.com/Neargye/magic_enum.git
        GIT_TAG v0.7.3)
FetchContent_GetProperties(magic_enum)
if(NOT magic_enum_POPULATED)
  FetchContent_Populate(magic_enum)
  add_subdirectory(${magic_enum_SOURCE_DIR} ${magic_enum_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

# === fmt ===
FetchContent_Declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG 11.0.2)
FetchContent_GetProperties(fmt)
if(NOT fmt_POPULATED)
  set(FMT_INSTALL ON)
  FetchContent_Populate(fmt)
  add_subdirectory(${fmt_SOURCE_DIR} ${fmt_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

# === spdlog ===
# Force spdlog to use downloaded fmt library
set(SPDLOG_FMT_EXTERNAL
    ON
    CACHE INTERNAL "") # Forces the value
FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG v1.14.1)
FetchContent_GetProperties(spdlog)
if(NOT spdlog_POPULATED)
  set(SPDLOG_INSTALL ON)
  FetchContent_Populate(spdlog)
  add_subdirectory(${spdlog_SOURCE_DIR} ${spdlog_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

# === boost ===
find_package(Boost REQUIRED COMPONENTS filesystem system thread regex)

# === nlohmannjson ===
set(JSON_ImplicitConversions OFF CACHE INTERNAL "")

FetchContent_Declare(json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG v3.11.3)
FetchContent_GetProperties(json)
if(NOT json_POPULATED)
  FetchContent_Populate(json)
  add_subdirectory(${json_SOURCE_DIR} ${json_BINARY_DIR})
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
install(TARGETS docopt DESTINATION lib)


# === websocket++ ===
FetchContent_Declare(websocketpp
GIT_REPOSITORY https://github.com/valerioformato/websocketpp.git
  GIT_TAG master)
FetchContent_GetProperties(websocketpp)
if(NOT websocketpp_POPULATED)
  FetchContent_Populate(websocketpp)
  add_subdirectory(${websocketpp_SOURCE_DIR} ${websocketpp_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()
# add interface library with all websocketpp dependencies
add_library(PMSWebsockets INTERFACE)
target_include_directories(PMSWebsockets INTERFACE ${websocketpp_SOURCE_DIR})
target_link_libraries(PMSWebsockets INTERFACE Boost::system Boost::thread Boost::regex)

# === mongocxx ===
FetchContent_Declare(mongo-cxx
GIT_REPOSITORY https://github.com/mongodb/mongo-cxx-driver.git
  GIT_TAG r3.9.0)
FetchContent_GetProperties(mongo-cxx)
if(NOT mongo-cxx_POPULATED)
  FetchContent_Populate(mongo-cxx)
  add_subdirectory(${mongo-cxx_SOURCE_DIR} ${mongo-cxx_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

# === XRootD ===
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake/Modules)
find_package(XROOTD)
add_library(PMSXrootd INTERFACE)
if(XROOTD_FOUND)
  message(STATUS "Enabling support for XRootD file transfer")
  target_compile_definitions(PMSXrootd INTERFACE ENABLE_XROOTD)
  target_include_directories(PMSXrootd INTERFACE ${XROOTD_INCLUDE_DIR})
  target_link_directories(PMSXrootd INTERFACE ${XROOTD_LIB_DIR})
  if(NOT APPLE)
    # we force the use of RPATH instead of RUNPATH so that all XRootD libraries will be found automatically
    target_link_options(PMSXrootd INTERFACE -Wl,--disable-new-dtags)
  endif()
  target_link_libraries(PMSXrootd INTERFACE XrdCl)
endif()

# find_package(bsoncxx REQUIRED)
# find_package(mongocxx REQUIRED 3.6.0)

# === Catch2 ===
if(ENABLE_TESTS)
FetchContent_Declare(catch2
GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG v3.6.0)
FetchContent_GetProperties(catch2)
if(NOT catch2_POPULATED)
  FetchContent_Populate(catch2)
  add_subdirectory(${catch2_SOURCE_DIR} ${catch2_BINARY_DIR} EXCLUDE_FROM_ALL)
  list(APPEND CMAKE_MODULE_PATH ${catch2_SOURCE_DIR}/extras)
endif()

FetchContent_Declare(trompeloeil
GIT_REPOSITORY https://github.com/rollbear/trompeloeil.git
  GIT_TAG v49)
FetchContent_GetProperties(trompeloeil)
if(NOT trompeloeil_POPULATED)
  FetchContent_Populate(trompeloeil)
  add_subdirectory(${trompeloeil_SOURCE_DIR} ${trompeloeil_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()
endif()
