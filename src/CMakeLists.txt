configure_file(PMSVersion.h.in PMSVersion.h @ONLY)

add_library(PMSVersion INTERFACE)
target_include_directories(PMSVersion INTERFACE ${CMAKE_CURRENT_BINARY_DIR})

add_subdirectory(db)
add_subdirectory(pilot)
add_subdirectory(orchestrator)
