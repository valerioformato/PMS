add_executable(PMSPilot main.cpp)
target_link_libraries(PMSPilot PRIVATE spdlog::spdlog docopt mongo::bsoncxx_shared mongo::mongocxx_shared nlohmann_json::nlohmann_json)