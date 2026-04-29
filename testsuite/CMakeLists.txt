add_executable(run_tests 
run_tests.cpp
common/testEnumArray.cpp
common/testQueue.cpp
db/testHarness.cpp
db/testMongoDBBackend.cpp
db/queries/testMatch.cpp
db/queries/testUpdateAction.cpp
utils/testUtils.cpp
orchestrator/testCommandParsing.cpp
orchestrator/testTask.cpp
orchestrator/testDirector.cpp
orchestrator/testHandleCommand.cpp
orchestrator/testOrchestratorConfig.cpp
)
target_include_directories(run_tests PRIVATE ${CMAKE_SOURCE_DIR}/testsuite)
target_link_libraries(run_tests Catch2::Catch2WithMain DBUtils PMSOrchestratorLib trompeloeil)

catch_discover_tests(run_tests)
