add_executable(run_tests 
run_tests.cpp
db/testHarness.cpp
db/testMongoDBBackend.cpp
utils/testUtils.cpp
)
target_include_directories(run_tests PRIVATE ${CMAKE_SOURCE_DIR}/testsuite)
target_link_libraries(run_tests Catch2::Catch2WithMain DBUtils trompeloeil)

catch_discover_tests(run_tests)
