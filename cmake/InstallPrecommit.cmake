find_program(PRECOMMIT pre-commit)
if (NOT PRECOMMIT)
    find_program(PIP NAMES pip pip3)
    message(STATUS "Will use ${PIP}")
    if (PIP)
        message(STATUS "pre-commit not found, trying to install...")
        execute_process(
                COMMAND ${PIP} install pre-commit
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
                RESULT_VARIABLE PRECOMMIT_INSTALLED
        )
        # RESULT_VARIABLE returns the exit code of the process!
        if (NOT PRECOMMIT_INSTALLED)
            message(STATUS "Installing pre-commit hooks...")
            execute_process(
                    COMMAND pre-commit install
                    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
                    RESULT_VARIABLE PRECOMMIT_HOOKS_INSTALLED
            )
        endif ()
    endif ()
endif ()
