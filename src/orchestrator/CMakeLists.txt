set(PMSOrchestrator_src 
    main.cpp
    Director.cpp
    OrchestratorConfig.cpp 
    Server.cpp
  )

add_executable(PMSOrchestrator ${PMSOrchestrator_src})
target_include_directories(PMSOrchestrator SYSTEM PRIVATE ${Boost_INCLUDE_DIR})
target_include_directories(PMSOrchestrator PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(PMSOrchestrator PRIVATE Boost::filesystem spdlog::spdlog docopt nlohmann_json::nlohmann_json PMSWebsockets MongoDBUtils)

install(TARGETS PMSOrchestrator DESTINATION bin)