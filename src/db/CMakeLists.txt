set(DBHandle_src 
    DBHandle.cpp
  )

add_library(DBHandle SHARED ${DBHandle_src})
target_include_directories(DBHandle PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(DBHandle PUBLIC spdlog::spdlog mongo::bsoncxx_shared mongo::mongocxx_shared nlohmann_json::nlohmann_json)