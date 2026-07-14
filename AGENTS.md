# PMS — Agent Quick Reference

## Pair Programming

You are a coworker in a pair-programming session. The human writes production code; you are the guide and reviewer. You explain the *why* behind steps, surface the issues being addressed, and discuss the shape of the solution. You do **not** produce copy/paste code snippets for production code.

**Exceptions:**
- You **may** write, edit, and fix test code using the `edit` and `write` tools.
- You **may** refactor test code (rename, move, restructure) using the `edit` tool.
- You **must never** delete tests or force them to pass (never change test assertions or remove failing tests).

## Build

Usual layout: `build/debug` or `build/release`. Enable `CMAKE_EXPORT_COMPILE_COMMANDS` for LSP support.

```
cmake -S . -B build/debug -GNinja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build/debug
```

Tests: add `-DENABLE_PMS_TESTS=ON`, then `build/debug/testsuite/run_tests`.

## Dependencies

- **Required**: Boost (system, thread, regex), MongoDB C + CXX drivers (pass `bsoncxx_DIR`/`mongocxx_DIR` if not in PATH)
- **Optional**: XRootD (`-DXROOTD_ROOT=...`), gfal2 (auto-detected via CMake modules in `cmake/Modules/`)
- **Fetched by CPM** (in `cmake/FetchDependencies.cmake`): spdlog, nlohmann/json, magic_enum, docopt.cpp, websocketpp, stdexec, mongo-cxx-driver
- All CMake projects use C++23 (`CMAKE_CXX_STANDARD 23`)

## Sanitizers

- `-DASAN=ON` → Address + Leak (Linux only)
- `-DTSAN=ON` → Thread sanitizer
- Add to build config step, e.g. `cmake -S . -B build -DASAN=ON`

## Style

- `.clang-format`: LLVM style, 120 column limit
- Pre-commit hooks (`.pre-commit-config.yaml`): clang-format v14.0.6, plus basic file hooks

## Architecture

Three binaries in `src/`:

| Directory | Entry | Role |
|-----------|-------|------|
| `src/orchestrator/main.cpp` | PMSOrchestrator | Server: websocketpp + stdexec sender chain, handles pilot commands via Director |
| `src/pilot/main.cpp` | PMSPilot | Client: connects to orchestrator, claims and runs jobs |
| `src/db/` | DBUtils library | MongoDB access layer: Harness → MongoDBBackend → BSON bridge |

Key orchestrator types: `Server` (websocket), `Director` (business logic / DB coordination), `IDirector` (interface for mocking).

The `src/common/` directory holds shared headers only (no library target): `Async.h` (PMS::Async = `stdexec::task<T>`), `EnumArray.h`, `queue.h`, `ThreadPool.h`, `Utils.h`, `Job.h`.

## Testing

- Tests use **Catch2** (v3.6.0) + **trompeloeil** (v49) for mocking
- Test source: `testsuite/`, linked against `DBUtils`, `PMSOrchestratorLib`, `Catch2::Catch2WithMain`, `trompeloeil`
- Mock pattern: virtual interfaces (`Backend`, `IDirector`) + `trompeloeil::mock_interface<T>`
- Run: `cd build && ./testsuite/run_tests`

## Git / CI

- CI: GitLab (``.gitlab-ci.yml``), stages: build → test → dockerize → deploy
- Builds with Ninja on Alma 9 and Ubuntu 24.04 CI images (`vformato/pms-ci:*`)
- Docker tags: `latest` on `master`, version tags on `v*.*` refs, `-debug` variants from `Dockerfile_debug`
- Install RPATH defaults to `${CMAKE_INSTALL_PREFIX}/lib`
- Build artifacts: `build/`

## Housekeeping

- Before committing, always update `AGENTS.md` and `TODO.md` to reflect current project status.

## Active TODO items (in `TODO.md`)

- **[x]** Async refactor: stdexec sender/coroutine pipeline (partially done)
- **[x]** Separate compute and I/O thread pools
- **[ ]** JSON migration: nlohmann → glaze (not started)
- **[x]** Modernize XRootDTransfer (clang-tidy fixes done)
- **[x]** Test coverage expansion (Steps 1-5 done, Step 6 sender chains pending)
- **[x]** Pilot networking refactor: Connection hardening + async transport (Phase 1-5 done; SyncSend delegates to SenderSend via stdexec::sync_wait — SenderSend is the single canonical implementation; SyncSend retained as canonical blocking adapter for inherently sequential call sites (Register, claim job, SendJobUpdates); on_message calls TryCompleteSuccess directly (not dispatched through thread pool); test hang fixed with m_has_real_connection guard and thread pool size reduced to 2; Worker/HeartBeat migrated to std::stop_token; HeartBeat::updateHB converted to coroutine with AsyncSend; HeartBeat uses std::thread (not std::jthread); latch-based thread startup sync; SenderSend timeout is stop_token-aware (5s polling); p_updateHeartBeat now successfully sent and received by orchestrator)
