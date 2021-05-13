set(PMSPilot_src 
    main.cpp 
    HeartBeat.cpp
    PilotConfig.cpp
    Worker.cpp
  )

add_executable(PMSPilot ${PMSPilot_src})
target_include_directories(PMSPilot SYSTEM PRIVATE ${Boost_INCLUDE_DIR})
target_include_directories(PMSPilot PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(PMSPilot PRIVATE Boost::filesystem spdlog::spdlog_header_only docopt nlohmann_json::nlohmann_json MongoDBUtils)