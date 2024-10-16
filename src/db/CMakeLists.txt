set(MongoDBUtils_src 
    backends/MongoDB/DBHandle.cpp
    backends/MongoDB/PoolHandle.cpp
  )

add_library(MongoDBUtils SHARED ${MongoDBUtils_src})
target_include_directories(MongoDBUtils PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(MongoDBUtils PUBLIC magic_enum::magic_enum spdlog::spdlog mongocxx_shared bsoncxx_shared nlohmann_json::nlohmann_json)
install(TARGETS MongoDBUtils DESTINATION lib)

set(DBUtils_src
    harness/Harness.cpp
    backends/MongoDB/MongoDBBackend.cpp
    )
add_library(DBUtils SHARED ${DBUtils_src})
target_include_directories(DBUtils PUBLIC ${CMAKE_SOURCE_DIR}/src)
# TODO: remove this dependency on MongoDBUtils (due to json conversion functions that should be used only in the MongoDB backend)
target_link_libraries(DBUtils PUBLIC MongoDBUtils)
install(TARGETS DBUtils DESTINATION lib)
    
