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
orchestrator/testSenderChain.cpp
orchestrator/testOrchestratorConfig.cpp
pilot/testMessageReply.cpp
pilot/testConnection.cpp
)
target_include_directories(run_tests PRIVATE ${CMAKE_SOURCE_DIR}/testsuite)
target_link_libraries(run_tests Catch2::Catch2WithMain DBUtils PMSOrchestratorLib PMSPilotLib trompeloeil)

catch_discover_tests(run_tests)
