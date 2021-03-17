set(PMSPilot_src 
    main.cpp 
    PilotConfig.cpp
  )

add_executable(PMSPilot ${PMSPilot_src})
target_include_directories(PMSPilot PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(PMSPilot PRIVATE DBHandle spdlog::spdlog docopt nlohmann_json::nlohmann_json)