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
orchestrator/testHttpHealth.cpp
orchestrator/testSenderChain.cpp
orchestrator/testOrchestratorConfig.cpp
pilot/testMessageReply.cpp
pilot/testConnection.cpp
pilot/testHeartBeat.cpp
)
target_include_directories(run_tests PRIVATE ${CMAKE_SOURCE_DIR}/testsuite)
target_link_libraries(run_tests Catch2::Catch2WithMain DBUtils PMSOrchestratorLib PMSPilotLib trompeloeil)

catch_discover_tests(run_tests PROPERTIES SKIP_RETURN_CODE 4)

add_test(NAME cli_orchestrator_help COMMAND $<TARGET_FILE:PMSOrchestrator> --help)
set_tests_properties(cli_orchestrator_help PROPERTIES PASS_REGULAR_EXPRESSION "PMS job orchestrator executable")

add_test(NAME cli_orchestrator_version COMMAND $<TARGET_FILE:PMSOrchestrator> --version)
set_tests_properties(cli_orchestrator_version PROPERTIES PASS_REGULAR_EXPRESSION "PMS v[0-9]+\\.[0-9]+\\.[0-9]+")

add_test(NAME cli_pilot_help COMMAND $<TARGET_FILE:PMSPilot> --help)
set_tests_properties(cli_pilot_help PROPERTIES PASS_REGULAR_EXPRESSION "PMS Pilot fish executable")

add_test(NAME cli_pilot_version COMMAND $<TARGET_FILE:PMSPilot> --version)
set_tests_properties(cli_pilot_version PROPERTIES PASS_REGULAR_EXPRESSION "PMS v[0-9]+\\.[0-9]+\\.[0-9]+")
