cmake_minimum_required(VERSION 3.13)

cmake_policy(SET CMP0111 OLD)

# Pilot Management System
project(PMS VERSION 0.0.2)

set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY TRUE)
# use, i.e. don't skip the full RPATH for the build tree
set(CMAKE_SKIP_BUILD_RPATH FALSE)

# when building, don't use the install RPATH already (but later on when installing)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

if (NOT DEFINED CMAKE_INSTALL_RPATH)
    set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
else ()
    message(WARNING "Manually specified install rpath: ${CMAKE_INSTALL_RPATH}")
endif ()

# add the automatically determined parts of the RPATH which point to directories outside the build tree to the install
# RPATH
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# Check for C++ standard and set it to C++17 if not already set
set(CMAKE_CXX_STANDARD 17)

# -------------------------------------------------------------------
# -------------------------- Begin project --------------------------
# -------------------------------------------------------------------

option(ASAN "Enable Address sanitizer" OFF)
option(TSAN "Enable Thread sanitizer" OFF)
option(DEBUG_WEBSOCKETS "Enable debug output for websocketpp" OFF)

if (ASAN)
    message(STATUS "Enabling Address sanitizer")
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)

    if (NOT APPLE)
        add_compile_options(-fsanitize=leak)
        add_link_options(-fsanitize=leak)
    endif ()
endif ()

if (TSAN)
    message(STATUS "Enabling Thread sanitizer")
    add_compile_options(-fsanitize=thread)
    add_link_options(-fsanitize=thread)
endif ()

add_compile_options(-Wall -Wextra)

# cmake_policy doesn't work, any call to cmake_minimum_required resets policies
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

include(cmake/FetchDependencies.cmake)
include(cmake/InstallPrecommit.cmake)

# Get git commit sha
execute_process(
        COMMAND git log -1 --format=%H
        WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
        OUTPUT_VARIABLE GIT_HASH
        OUTPUT_STRIP_TRAILING_WHITESPACE
)

add_subdirectory(src)

# add_subdirectory(doc)
