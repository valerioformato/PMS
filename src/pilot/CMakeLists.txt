set(PMSPilot_lib_src 
    HeartBeat.cpp
    PilotConfig.cpp
    Worker.cpp
    client/Client.cpp
    client/Connection.cpp
    filetransfer/FileTransferQueue.cpp
    filetransfer/GfalFileTransfer.cpp
    )

if(XROOTD_FOUND)
  set(PMSPilot_lib_src 
      ${PMSPilot_lib_src}     
      filetransfer/XRootDFileTransfer.cpp
  )
endif()

add_library(PMSPilotLib ${PMSPilot_lib_src})
target_include_directories(PMSPilotLib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(PMSPilotLib PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_include_directories(PMSPilotLib SYSTEM PRIVATE ${Boost_INCLUDE_DIR})
target_link_libraries(PMSPilotLib PRIVATE Boost::filesystem magic_enum::magic_enum spdlog::spdlog_header_only nlohmann_json::nlohmann_json PMSVersion PMSWebsockets)

if (DEBUG_WEBSOCKETS)
    target_compile_definitions(PMSPilotLib PRIVATE DEBUG_WEBSOCKETS)
endif()

if(XROOTD_FOUND)
  message("Linking pilot with XRootD")
  target_link_libraries(PMSPilotLib PRIVATE PMSXrootd)
endif()

if(GFAL2_FOUND)
  message("Linking pilot with gfal2")
  target_link_libraries(PMSPilotLib PRIVATE PMSgfal2)
endif()

add_executable(PMSPilot main.cpp)
target_include_directories(PMSPilot SYSTEM PRIVATE ${Boost_INCLUDE_DIR})
target_include_directories(PMSPilot PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(PMSPilot PRIVATE docopt PMSPilotLib)
target_compile_definitions(PMSPilot PRIVATE FMT_HEADER_ONLY)

install(TARGETS PMSPilot DESTINATION bin)
