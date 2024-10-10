set(PMSPilot_src 
    main.cpp 
    HeartBeat.cpp
    PilotConfig.cpp
    Worker.cpp
    client/Client.cpp
    client/Connection.cpp
    filetransfer/FileTransferQueue.cpp
    )

if(XROOTD_FOUND)
  set(PMSPilot_src 
      ${PMSPilot_src}     
      filetransfer/XRootDFileTransfer.cpp
  )
endif()

add_executable(PMSPilot ${PMSPilot_src})
target_include_directories(PMSPilot SYSTEM PRIVATE ${Boost_INCLUDE_DIR})
target_include_directories(PMSPilot PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(PMSPilot PRIVATE Boost::filesystem fmt::fmt-header-only spdlog::spdlog_header_only docopt nlohmann_json::nlohmann_json PMSVersion PMSWebsockets MongoDBUtils)

if (DEBUG_WEBSOCKETS)
    target_compile_definitions(PMSPilot PRIVATE DEBUG_WEBSOCKETS)
endif()


if(XROOTD_FOUND)
  target_link_libraries(PMSPilot PRIVATE PMSXrootd)
endif()

install(TARGETS PMSPilot DESTINATION bin)
