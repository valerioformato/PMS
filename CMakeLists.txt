cmake_minimum_required(VERSION 3.13)

# Ntuples (for) AMS-Italy Analysis
project(PMS)

set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY TRUE)
# use, i.e. don't skip the full RPATH for the build tree
set(CMAKE_SKIP_BUILD_RPATH FALSE)

# when building, don't use the install RPATH already (but later on when installing)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

# add the automatically determined parts of the RPATH which point to directories outside the build tree to the install
# RPATH
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# Check for C++ standard and set it to C++14 if not already set
set(CMAKE_CXX_STANDARD_REQUIRED 14)
set(CMAKE_CXX_STANDARD 14)

# -------------------------------------------------------------------
# -------------------------- Begin project --------------------------
# -------------------------------------------------------------------

if(DEFINED ASAN)
  add_compile_options(-fsanitize=address -fsanitize=leak)
  add_link_options(-fsanitize=address -fsanitize=leak)
endif()

add_compile_options(-Wall -Wextra)

# cmake_policy doesn't work, any call to cmake_minimum_required resets policies
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

include(cmake/FetchDependencies.cmake)


add_subdirectory(src)

# add_subdirectory(doc)
