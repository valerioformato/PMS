add_executable(run_tests 
run_tests.cpp
db/testHarness.cpp
)
target_include_directories(run_tests PRIVATE ${CMAKE_SOURCE_DIR}/testsuite)
target_link_libraries(run_tests Catch2::Catch2WithMain MongoDBUtils trompeloeil)

catch_discover_tests(run_tests)
