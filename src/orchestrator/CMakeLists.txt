set(PMSOrchestratorLib_src
    CommandParser.cpp
    Director.cpp
    OrchestratorConfig.cpp
    Server.cpp
)

add_library(PMSOrchestratorLib STATIC ${PMSOrchestratorLib_src})
target_include_directories(PMSOrchestratorLib PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_include_directories(PMSOrchestratorLib SYSTEM PUBLIC ${Boost_INCLUDE_DIR})
target_link_libraries(PMSOrchestratorLib PUBLIC STDEXEC::stdexec spdlog::spdlog_header_only nlohmann_json::nlohmann_json magic_enum::magic_enum PMSWebsockets DBUtils)
target_compile_definitions(PMSOrchestratorLib PUBLIC FMT_HEADER_ONLY)

if (DEBUG_WEBSOCKETS)
  target_compile_definitions(PMSOrchestratorLib PRIVATE DEBUG_WEBSOCKETS)
endif()

add_executable(PMSOrchestrator main.cpp)
target_link_libraries(PMSOrchestrator PRIVATE PMSOrchestratorLib cxxopts::cxxopts PMSVersion)

install(TARGETS PMSOrchestrator DESTINATION bin)
