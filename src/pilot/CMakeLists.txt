add_executable(PMSPilot main.cpp)
target_link_libraries(PMSPilot PRIVATE spdlog::spdlog docopt)