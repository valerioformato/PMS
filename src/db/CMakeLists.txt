set(MongoDBUtils_src 
    DBHandle.cpp
    PoolHandle.cpp
  )

add_library(MongoDBUtils SHARED ${MongoDBUtils_src})
target_include_directories(MongoDBUtils PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(MongoDBUtils PUBLIC magic_enum::magic_enum spdlog::spdlog mongo::mongocxx_shared mongo::bsoncxx_shared nlohmann_json::nlohmann_json)
install(TARGETS MongoDBUtils DESTINATION lib)
