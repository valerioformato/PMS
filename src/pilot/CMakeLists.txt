set(PMSPilot_src 
    main.cpp 
    HeartBeat.cpp
    PilotConfig.cpp
    Worker.cpp
    client/Client.cpp
    client/Connection.cpp
    filetransfer/FileTransferQueue.cpp
    filetransfer/GfalFileTransfer.cpp
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
target_link_libraries(PMSPilot PRIVATE Boost::filesystem magic_enum::magic_enum spdlog::spdlog_header_only docopt nlohmann_json::nlohmann_json PMSVersion PMSWebsockets)

if (DEBUG_WEBSOCKETS)
    target_compile_definitions(PMSPilot PRIVATE DEBUG_WEBSOCKETS)
endif()


if(XROOTD_FOUND)
  message("Linking pilot with XRootD")
  target_link_libraries(PMSPilot PRIVATE PMSXrootd)
endif()

if(GFAL2_FOUND)
  message("Linking pilot with gfal2")
  target_link_libraries(PMSPilot PRIVATE PMSgfal2)
endif()

install(TARGETS PMSPilot DESTINATION bin)
