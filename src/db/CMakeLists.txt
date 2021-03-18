set(DBHandle_src 
    DBHandle.cpp
  )

add_library(DBHandle SHARED ${DBHandle_src})
target_include_directories(DBHandle PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(DBHandle PUBLIC spdlog::spdlog mongo::mongocxx_shared mongo::bsoncxx_shared  nlohmann_json::nlohmann_json)