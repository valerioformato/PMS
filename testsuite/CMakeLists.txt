add_executable(run_tests run_tests.cpp)
target_link_libraries(run_tests Catch2::Catch2WithMain)

catch_discover_tests(run_tests)
