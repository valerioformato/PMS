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
  GIT_TAG 7.1.3)
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
  GIT_TAG v1.8.5)
FetchContent_GetProperties(spdlog)
if(NOT spdlog_POPULATED)
  set(SPDLOG_INSTALL ON)
  FetchContent_Populate(spdlog)
  add_subdirectory(${spdlog_SOURCE_DIR} ${spdlog_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

# === boost ===
find_package(Boost REQUIRED COMPONENTS filesystem system thread regex)

# === nlohmannjson ===
FetchContent_Declare(json
  GIT_REPOSITORY https://github.com/ArthurSonzogni/nlohmann_json_cmake_fetchcontent.git
  GIT_TAG v3.7.3)
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
GIT_REPOSITORY https://github.com/zaphoyd/websocketpp.git
  GIT_TAG 0.8.2)
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
# add interface library with all websocketpp dependencies
# add_library(PMSWebsockets INTERFACE)
# target_include_directories(PMSWebsockets INTERFACE ${websocketpp_SOURCE_DIR})
# target_link_libraries(PMSWebsockets INTERFACE Boost::system Boost::thread Boost::regex)

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
