# handle dependencies
include(cmake/CPM.cmake)


# needed for the fmt and spdlog static libraries
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
# needed to avoid implicit conversions for json documents
set(JSON_ImplicitConversions OFF CACHE INTERNAL "")

CPMAddPackage("gh:Neargye/magic_enum#v0.9.7")
CPMAddPackage("gh:gabime/spdlog#v1.16.0")
CPMAddPackage(
  URI "gh:nlohmann/json#v3.11.3"
  OPTIONS "JSON_ImplicitConversions OFF" 
)
CPMAddPackage(
  URI "gh:jarro2783/cxxopts#v3.3.1"
  OPTIONS "CXXOPTS_BUILD_PYTHON OFF"
)
CPMAddPackage("gh:valerioformato/websocketpp#boost")
CPMAddPackage(
  NAME mongo-cxx-driver
  GITHUB_REPOSITORY mongodb/mongo-cxx-driver
  GIT_TAG r4.4.1
  EXCLUDE_FROM_ALL NO
  SYSTEM YES
)
CPMAddPackage(
	URI "gh:nvidia/stdexec#gtc-2026"
	OPTIONS "STDEXEC_BUILD_EXAMPLES OFF"
)
# === boost ===
set(BOOST_COMPONENTS filesystem thread regex)
find_package(Boost COMPONENTS ${BOOST_COMPONENTS} REQUIRED)

# add interface library with all websocketpp dependencies
add_library(PMSWebsockets INTERFACE)
target_include_directories(PMSWebsockets INTERFACE ${websocketpp_SOURCE_DIR})
target_link_libraries(PMSWebsockets INTERFACE Boost::headers Boost::thread Boost::regex)

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
  if(GFAL2_LIBRARY_DIRS)
    target_link_directories(PMSgfal2 INTERFACE ${GFAL2_LIBRARY_DIRS})
  endif()
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
