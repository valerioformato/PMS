# TODO

## Async refactor: full sender/coroutine pipeline

**Goal**: make the entire request → processing → DB → response flow async using stdexec senders and `exec::task` coroutines.

### Strategy

- `mongocxx` is synchronous/blocking — "async" here means dispatching blocking work to a thread pool, not true async I/O. This is the right approach for a blocking driver.
- Use **scheduler injection via the environment** (`ex::read_env(ex::get_scheduler)`) in the Harness rather than storing a scheduler instance — keeps the DB layer decoupled from any specific thread pool.
- Use `ex::start_detached` in `message_handler` instead of `ex::sync_wait`, moving the `m_endpoint.send(...)` call **inside** the sender chain as a final `then` step. The websocket callback returns immediately.
- Launch the whole chain with `ex::on(m_thread_pool.get_scheduler(), ...)` so the scheduler flows through automatically to all nested senders.

### Threading model

- **websocketpp threads**: only dispatch — `message_handler` fires work to the stdexec pool and returns in microseconds, always free to accept new connections and messages.
- **stdexec pool workers** (`m_thread_pool`): do all heavy lifting — JSON processing, DB calls, sending responses.
- This means the server can receive and dispatch new requests even while processing a large backlog. Throughput ceiling is pool size + DB latency, not websocket thread availability.
- Once `start_detached` is in place, `m_threadPool` (the old `Thread::Pool`) can likely be retired — websocket callbacks will no longer need it.
- `websocketpp::send()` is thread-safe (posts through the asio strand internally), so calling it from a stdexec pool thread is safe.
- The `hdl` captured in the `start_detached` lambda remains valid as long as the connection is open. Connection lifetime is independent of `message_handler` returning — the connection stays open until a close frame is exchanged or a network error occurs.

### Separate compute and I/O thread pools

`mongocxx` is a blocking driver. Even with a fully sender-based pipeline, a thread is held for the entire DB roundtrip whenever a DB call is made via `ex::then`. Under load (many pilots, high DB latency), compute threads pile up waiting on MongoDB and fast operations (liveness probes, heartbeats) queue behind them — the same problem as the old `Thread::Pool` model.

**Fix**: split `m_thread_pool` into two distinct stdexec pools:

- **Compute pool** (`hardware_concurrency` threads): JSON parsing, command dispatch, response serialization — pure CPU, microseconds per task.
- **I/O pool** (`~4× hardware_concurrency` threads, co-configured with `mongocxx::pool` size): all blocking DB calls, scheduled via `ex::on(io_scheduler, ...)` at the Harness layer.

The `ex::read_env(ex::get_scheduler)` injection point already planned in the Harness step (Step 1 above) naturally supports this: the Harness picks up whichever scheduler is in its environment. The Server injects the compute scheduler at the top of the pipeline; the Harness switches to the I/O scheduler internally for each DB operation and resumes on the compute scheduler when done.

Sizing guidance: I/O pool thread count should equal `mongocxx::pool` max size — more I/O threads than DB connections gain nothing (threads queue on the connection pool). Expose both as `OrchestratorConfig` fields with sensible defaults.

### Steps

1. **Harness**: add `RunQuery` / `BulkWrite` overloads returning `sender<ErrorOr<QueryResult>>` by scheduling on the ambient scheduler via `ex::read_env(ex::get_scheduler)`.

2. **Director**: migrate methods (`CreateTask`, `AddNewJob`, `PilotClaimJob`, etc.) to return `exec::task<ErrorOr<T>>`, using `co_await` on the sender-returning Harness methods.

3. **Server::HandleCommand(UserCommand&&)**: return `exec::task<std::string>` instead of `std::string`. The `then` step in `message_handler` becomes `let_value` to await the task.

4. **Server::message_handler**: replace `ex::sync_wait` with `ex::start_detached`, move `m_endpoint.send(hdl, ...)` inside the chain as a final `then` step, and wrap everything with `ex::on(m_thread_pool.get_scheduler(), ...)`.

5. **Retire `m_threadPool`** (`Thread::Pool`): remove once websocket callbacks no longer dispatch to it.

6. **Director background threads** (`JobInsert`, `JobTransfer`, etc.): decide whether to migrate to senders/coroutines or leave as-is. Note: `mongocxx::pool` is thread-safe so coexistence is not a correctness issue, but contention is worth profiling.

### Open questions

- Should `pilot_handler` be migrated in the same pass?
- Should `Director`'s background threads (`JobInsert`, `JobTransfer`, `UpdateTasks`, etc.) also be converted to `exec::task` coroutines scheduled on the same pool?

---

## JSON library migration: nlohmann → glaze

**Goal**: replace `nlohmann::json` with [glaze](https://github.com/stephenberry/glaze), using static struct-based serialization where schemas are known and `glz::generic` only where truly dynamic.

### Strategy

- **Do not do a straight `nlohmann::json` → `glz::generic` swap.** Glaze's primary model is static, compile-time struct reflection. Using `glz::generic` everywhere would be a lateral move with modest gains.
- Introduce dedicated C++ structs for all domains with known schemas and let glaze reflect them statically. Push `glz::generic` down to only the genuinely dynamic boundary: the DB layer.
- The two modes (`glz::read_json<MyStruct>` and `glz::read_json<glz::generic>`) coexist cleanly — mix freely.

### Migration order

1. **Command parsing** (`UserCommands`, `Commands.h`, `toUserCommand`): define static structs for all user commands. Glaze supports tagged variant dispatch — annotating `UserCommand` with the `"command"` discriminator field can replace the entire `toUserCommand()` dispatch function with a single `glz::read_json<UserCommand>(payload)` call.

2. **`OrchestratorConfig`**: currently uses `infile >> configJson` (nlohmann stream operator, no glaze equivalent). Move to a static config struct and `glz::read_file_json` or explicit file-read + `glz::read_json`.

3. **Command response types**: ad-hoc JSON objects built in `HandleCommand` can become typed response structs serialized with `glz::write_json`.

4. **DB layer** (`MongoDBBackend` BSON bridge, `QueryResult`): keep as `glz::generic`. The BSON bridge already goes through a string round-trip (`bsoncxx::to_json` / `bsoncxx::from_json`), so `glz::write_json` / `glz::read_json<glz::generic>` slot in with minimal change. `QueryResult` field names are runtime-determined — dynamic is appropriate here.

### Key API differences to handle

- `.dump()` → `glz::write_json(obj)` returns `expected<string, glz::error_ctx>`. Fits naturally into the existing `ErrorOr` error handling model.
- `json::parse(str)` → `glz::read_json<glz::generic>(str)` (also returns `expected`).
- `.items()` iteration (4 usages in `Match.cpp` / `Update.cpp`): replace with iteration over the underlying map type (e.g. `for (auto& [k, v] : obj.get_object())`). The tricky case is `value.items().begin()` structured-binding (nlohmann iterator quirk) — replace with `*std::begin(obj.get_object())`.
- `_json` UDL (~10 usages): replace with `glz::read_json<glz::generic>(R"(...)").value()`.
- `infile >> configJson` stream parsing: read file manually then call `glz::read_json`.
- Dynamic nested assignment (`j["a"]["b"] = x`): verify that `glz::generic` auto-creates intermediate objects the same way nlohmann does before relying on this pattern.

### Defer

- **Glaze experimental websocket/HTTP server**: the README explicitly warns the API is under active development and likely to change. Do not replace websocketpp until glaze networking is stable. Evaluate separately after the JSON migration settles.

---

## Modernize XRootDTransfer code

**Goal**: apply C++ modernization fixes to `src/pilot/filetransfer/XRootDFileTransfer.cpp` (and its companion header `FileTransferQueue.h`) using clang-tidy's `modernize-*` checks.

### Checks to apply

Run with:
```
clang-tidy -p build/debug/compile_commands.json \
  -checks='-*,modernize-*,-modernize-use-trailing-return-type' \
  --header-filter='src/pilot/.*' \
  src/pilot/filetransfer/XRootDFileTransfer.cpp
```

`modernize-use-trailing-return-type` is excluded — it is purely stylistic and would produce noise without improving readability for this codebase.

### Known findings (as of initial audit)

| Line | Check | Description |
|---|---|---|
| L40 (`IndexRemote`) | `modernize-use-nullptr` | `XrdCl::DirectoryList *dirList = 0` → `nullptr` |
| L110, L116 (`AddXRootDFileTransfer`) | `modernize-use-starts-ends-with` | `rfind("//") == length() - 2` → `ends_with("//")` |
| L172, L178 (`AddXRootDFileTransfer`) | `modernize-use-emplace` | `push_back(std::string{...})` → `emplace_back(...)` |
| L183 (`AddXRootDFileTransfer`) | `modernize-use-auto` | `XrdCl::PropertyList *results = new XrdCl::PropertyList` → `auto *results = new ...` |
| L293 (`IndexXRootDRemote`) | `modernize-use-ranges` | `std::for_each(begin(...), end(...), ...)` → `std::ranges::for_each(...)` |

### Steps

1. Re-run clang-tidy with the command above to confirm the current finding list.
2. Apply fixes with `-fix` flag (or manually), excluding `modernize-use-trailing-return-type`.
3. Build and verify no regressions.

---

## Test coverage expansion

**Current state**: 124 assertions across 9 test cases. Coverage is limited to the DB query builders, `Harness`, `MongoDBBackend` helpers, and `Utils`. The orchestrator layer (`Server`, `Director`, `Task`) has zero test coverage.

### Prerequisite refactor: extract `IDirector` interface

Extract `IDirector` as an abstract base class in `Director.h` (or a dedicated `IDirector.h`) with all public `Director` methods as pure virtuals. `Director` implements `IDirector`. `Server` holds `std::shared_ptr<IDirector>`.

**Rationale**: consistent with the existing `Backend` / `MockBackend` pattern (already a virtual interface mocked via `trompeloeil::mock_interface<Backend>`). Keeps `Server` a concrete class (avoids forcing its substantial `.cpp` implementation into headers). The virtual dispatch cost is irrelevant for a service object called once per request. An explicit interface will also make the future async refactor easier — changing what Director methods return (e.g. to senders) is a single, well-scoped change.

`MockDirector` will use `trompeloeil::mock_interface<IDirector>`, mirroring the existing `MockBackend`.

### Step 1 — `Task` struct (pure logic, no deps)

New file: `testsuite/orchestrator/testTask.cpp`

All four predicates tested across boundary conditions:
- `IsActive()`: zero jobs, jobs present + readyForScheduling, readyForScheduling=false, all done/failed
- `IsFinished()`: totJobs=0 (false), all Done, partial Done
- `IsExhausted()`: pending=0 and error=0 (true), pending > 0 (false)
- `IsFailed()`: exhausted + failed > 0 (true), exhausted + failed = 0 (false)

### Step 2 — `EnumArray` and `ts_queue`

New files: `testsuite/common/testEnumArray.cpp`, `testsuite/common/testQueue.cpp`

- `EnumArray`: enum-keyed access, default construction, value assignment, out-of-bounds check
- `ts_queue`: push/pop, empty(), size(), `consume_all()` drains queue correctly, concurrent pushes from N threads (all items recovered)

### Step 3 — `toUserCommand` and `toPilotCommand`

Extract both static methods from `Server` into free functions (or a `CommandParser` namespace) to make them independently testable without constructing a full `Server` instance. This extraction also helps with the future glaze migration.

New file: `testsuite/orchestrator/testCommandParsing.cpp`

Cover for both user and pilot sides:
- Non-object JSON input → `InvalidCommand`
- Missing `"command"` field → `InvalidCommand`
- Non-string `"command"` field → `InvalidCommand`
- Empty string command → `InvalidCommand`
- Unknown command name → `InvalidCommand`
- `livenessProbe` detection
- Each valid command with all required fields present → correct variant
- Each valid command with a required field missing → `InvalidCommand`

### Step 4 — `Director` with mocked Harness

Add a constructor overload to `Director`: `Director(unique_ptr<Harness> frontDB, unique_ptr<Harness> backDB)` for test injection (alongside the existing `SetFrontDB`/`SetBackDB` approach). Use `MockBackend` → `Harness` → `Director`, mirroring the existing `testHarness.cpp` pattern.

New file: `testsuite/orchestrator/testDirector.cpp`

| Method | Cases |
|---|---|
| `CreateTask` | success (token returned, DB insert called); duplicate (error, no insert) |
| `ValidateTaskToken` | known task + correct token → Success; correct task + wrong token → ProcessError; unknown task → DatabaseError |
| `UpdateJobStatus` | unknown pilot → error; pilot not authorized for task → error; Running → startTime injected; Done/Error → endTime injected |
| `RegisterNewPilot` | all tasks valid; mixed valid/invalid (split correctly); zero valid tasks |
| `PilotClaimJob` | unknown pilot; all tasks inactive → finished reply; all tasks exhausted → sleep reply; happy-path claim |
| `DeleteHeartBeat` | removed from `m_activePilots`, DB Delete called |
| `AddTaskDependency` | in-memory task updated, DB Update called |
| `ClearTask` | `deleteTask=true`: both DBs cleared + task map entry removed; `deleteTask=false`: jobs cleared only |

### Step 5 — `Server::HandleCommand` with `MockDirector`

Depends on the `IDirector` prerequisite refactor.

New file: `testsuite/orchestrator/testHandleCommand.cpp`

One scenario per variant arm for both `HandleCommand(UserCommand&&)` and `HandleCommand(PilotCommand&&)`. Key cases:
- `SubmitJob`: SHA256 hash is injected into the job and appears in the reply
- `ClearTask`/`CleanTask`: token validation failure short-circuits (Director never called)
- `InvalidCommand`: returns the `errorMessage` field verbatim
- `LivenessProbe`: returns `"OK"`, Director never called

### Step 6 — `message_handler` / `pilot_handler` sender chains

Extract the sender pipeline construction from the websocket callback into a standalone function returning a sender, to allow testing without a live websocket. Then test:
- Malformed JSON → `upon_error` fires → error reply returned
- Valid JSON, unknown command → `InvalidCommand` flows through, reply returned
- Valid JSON, known command → expected reply content

### Priority order

| Priority | Step | Effort | Prerequisite |
|---|---|---|---|
| 1 | Step 1 (`Task`) | Small | None |
| 1 | Step 2 (`EnumArray`, `ts_queue`) | Small | None |
| 2 | Step 3 (`toUserCommand`/`toPilotCommand`) | Medium | Extract as free functions |
| 2 | Step 4 (`Director`) | Medium–Large | Injection constructor |
| 3 | Step 5 (`HandleCommand`) | Medium | `IDirector` interface |
| 3 | Step 6 (sender chains) | Medium | Pipeline extraction |
