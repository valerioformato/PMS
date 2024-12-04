# handle dependencies
include(FetchContent)

# needed for the fmt and spdlog static libraries
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# https://github.com/Neargye/magic_enum.git
# === magic_enum ===
FetchContent_Declare(
        magic_enum
        GIT_REPOSITORY https://github.com/Neargye/magic_enum.git
        GIT_TAG v0.7.3
        EXCLUDE_FROM_ALL)
FetchContent_GetProperties(magic_enum)
if(NOT magic_enum_POPULATED)
  FetchContent_MakeAvailable(magic_enum)
endif()

# === fmt ===
FetchContent_Declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG 11.0.2
  EXCLUDE_FROM_ALL)
FetchContent_GetProperties(fmt)
if(NOT fmt_POPULATED)
  set(FMT_INSTALL ON)
  FetchContent_MakeAvailable(fmt)
endif()

# === spdlog ===
# Force spdlog to use downloaded fmt library
set(SPDLOG_FMT_EXTERNAL
    ON
    CACHE INTERNAL "") # Forces the value
FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG v1.14.1
  EXCLUDE_FROM_ALL)
FetchContent_GetProperties(spdlog)
if(NOT spdlog_POPULATED)
  set(SPDLOG_INSTALL ON)
  FetchContent_MakeAvailable(spdlog)
endif()

# === boost ===
find_package(Boost REQUIRED COMPONENTS filesystem system thread regex)

# === nlohmannjson ===
set(JSON_ImplicitConversions OFF CACHE INTERNAL "")

FetchContent_Declare(json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG v3.11.3
  EXCLUDE_FROM_ALL)
FetchContent_GetProperties(json)
if(NOT json_POPULATED)
  FetchContent_MakeAvailable(json)
endif()

# === docopt ===
FetchContent_Declare(docopt
GIT_REPOSITORY https://github.com/docopt/docopt.cpp
  GIT_TAG v0.6.3
  EXCLUDE_FROM_ALL)
FetchContent_GetProperties(docopt)
if(NOT docopt_POPULATED)
  FetchContent_MakeAvailable(docopt)
endif()
install(TARGETS docopt DESTINATION lib)


# === websocket++ ===
FetchContent_Declare(websocketpp
GIT_REPOSITORY https://github.com/valerioformato/websocketpp.git
  GIT_TAG master
  EXCLUDE_FROM_ALL)
FetchContent_GetProperties(websocketpp)
if(NOT websocketpp_POPULATED)
  FetchContent_MakeAvailable(websocketpp)
endif()
# add interface library with all websocketpp dependencies
add_library(PMSWebsockets INTERFACE)
target_include_directories(PMSWebsockets INTERFACE ${websocketpp_SOURCE_DIR})
target_link_libraries(PMSWebsockets INTERFACE Boost::system Boost::thread Boost::regex)

# === mongocxx ===
set(ENABLE_TESTS
    OFF
    CACHE INTERNAL "") # Forces the value
set(BUILD_SHARED_LIBS_WITH_STATIC_MONGOC ON CACHE INTERNAL "")
FetchContent_Declare(mongo-cxx
GIT_REPOSITORY https://github.com/mongodb/mongo-cxx-driver.git
  GIT_TAG r3.9.0
  EXCLUDE_FROM_ALL)
FetchContent_GetProperties(mongo-cxx)
if(NOT mongo-cxx_POPULATED)
  FetchContent_MakeAvailable(mongo-cxx)
  install(TARGETS bsoncxx_shared mongocxx_shared DESTINATION lib)
endif()

# non-cmake packages
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake/Modules)
# === XRootD ===
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

find_package(GFAL2)
add_library(PMSgfal2 INTERFACE)
if(GFAL2_FOUND)
  message(STATUS "Enabling support for gfal file transfer")
  target_compile_definitions(PMSgfal2 INTERFACE ENABLE_GFAL2)
  target_include_directories(PMSgfal2 INTERFACE ${GFAL2_INCLUDE_DIR})
  if(NOT APPLE)
    # we force the use of RPATH instead of RUNPATH so that all gfal2 libraries will be found automatically
    target_link_options(PMSgfal2 INTERFACE -Wl,--disable-new-dtags)
  endif()

  find_package(PkgConfig REQUIRED)
  pkg_check_modules(gfal2_deps REQUIRED IMPORTED_TARGET glib-2.0)

  target_link_libraries(PMSgfal2 INTERFACE PkgConfig::gfal2_deps ${GFAL2_LIBRARIES})
endif()


# === Catch2 ===
if(ENABLE_PMS_TESTS)
FetchContent_Declare(catch2
GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG v3.6.0
  EXCLUDE_FROM_ALL)
FetchContent_GetProperties(catch2)
if(NOT catch2_POPULATED)
  FetchContent_MakeAvailable(catch2)
  list(APPEND CMAKE_MODULE_PATH ${catch2_SOURCE_DIR}/extras)
endif()

FetchContent_Declare(trompeloeil
GIT_REPOSITORY https://github.com/rollbear/trompeloeil.git
  GIT_TAG v49
  EXCLUDE_FROM_ALL)
FetchContent_GetProperties(trompeloeil)
if(NOT trompeloeil_POPULATED)
  FetchContent_MakeAvailable(trompeloeil)
endif()
endif()
