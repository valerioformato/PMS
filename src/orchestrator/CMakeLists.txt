set(PMSOrchestratorLib_src
    Director.cpp
    OrchestratorConfig.cpp
    Server.cpp
)

add_library(PMSOrchestratorLib STATIC ${PMSOrchestratorLib_src})
target_include_directories(PMSOrchestratorLib PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_include_directories(PMSOrchestratorLib SYSTEM PUBLIC ${Boost_INCLUDE_DIR})
target_link_libraries(PMSOrchestratorLib PUBLIC STDEXEC::stdexec spdlog::spdlog_header_only nlohmann_json::nlohmann_json PMSWebsockets DBUtils)
target_compile_definitions(PMSOrchestratorLib PUBLIC FMT_HEADER_ONLY)

if (DEBUG_WEBSOCKETS)
  target_compile_definitions(PMSOrchestratorLib PRIVATE DEBUG_WEBSOCKETS)
endif()

add_executable(PMSOrchestrator main.cpp)
target_link_libraries(PMSOrchestrator PRIVATE PMSOrchestratorLib docopt PMSVersion)

install(TARGETS PMSOrchestrator DESTINATION bin)
