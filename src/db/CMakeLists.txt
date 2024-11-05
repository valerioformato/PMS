set(MongoDBBackend_src
backends/MongoDB/PoolHandle.cpp
backends/MongoDB/DBHandle.cpp
backends/MongoDB/MongoDBBackend.cpp    
)

set(DBUtils_src
    harness/Harness.cpp
    queries/Match.cpp
    queries/Update.cpp
    ${MongoDBBackend_src}
    )
add_library(DBUtils SHARED ${DBUtils_src})
target_include_directories(DBUtils PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(DBUtils PUBLIC spdlog::spdlog magic_enum::magic_enum mongocxx_shared bsoncxx_shared nlohmann_json::nlohmann_json)
install(TARGETS DBUtils DESTINATION lib)
    
# target_link_libraries(MongoDBUtils PUBLIC Boost::boost Boost::system magic_enum::magic_enum spdlog::spdlog mongocxx_shared bsoncxx_shared nlohmann_json::nlohmann_json)
