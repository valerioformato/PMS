# TODO

## CLI parser migration: docopt.cpp → cxxopts

**Goal**: replace the unmaintained docopt.cpp parser with the header-only cxxopts library in both executables while
preserving their command-line interfaces.

- [x] Replace the CPM dependency with cxxopts v3.3.1 and link both executable targets against `cxxopts::cxxopts`.
- [x] Migrate `PMSOrchestrator` parsing for the positional config file, repeatable verbosity, help, and version.
- [x] Migrate `PMSPilot` parsing for the shared options plus `--maxJobs` and `--maxTime`.
- [x] Reject parser errors, missing config files, and surplus positional arguments with a nonzero exit status.
- [x] Add CTest smoke coverage for help and version handling in both executables.
- [x] Run tests through CTest in CI, including Catch2 discovery and correct handling of opt-in skipped tests.

---

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

## Separate compute and I/O thread pools

**Goal**: prevent MongoDB roundtrips from blocking compute threads by dispatching all DB calls to a dedicated I/O pool and resuming on the compute pool when they return.

### Design decisions

- **`Harness` stays scheduler-agnostic.** The existing `RunQuery(scheduler, q)` / `BulkWrite(scheduler, table, q)` overloads already accept any scheduler — no changes there.
- **All scheduling policy lives in `Director`.** Each coroutine method captures the ambient (compute) scheduler via `read_env(get_scheduler)`, dispatches DB work onto `m_io_thread_pool.get_scheduler()`, then hops back via `continues_on`.
- **Background threads** (`JobInsert`, `RunClaimQueries`, etc.) call the sync `RunQuery(q)` overload directly and already acquire their own `mongocxx::pool` entry per call — no changes needed.
- **Destruction order**: `m_io_thread_pool` must be declared in `Director` **before** `m_frontDB`/`m_backDB` so the pool outlives the DB handles (members are destroyed in reverse declaration order).

### Steps

- [x] **1. `OrchestratorConfig`**: add `nIOThreads` field (default: `4 × hardware_concurrency`). Co-size with `mongocxx::pool` max connections — more I/O threads than DB connections gain nothing.

- [x] **2. `Director.h`**: add `exec::static_thread_pool m_io_thread_pool` declared **before** `m_frontDB`/`m_backDB`. Use `m_io_thread_pool.get_scheduler()` at each DB call site.

- [x] **3. `Director` helper**: resolved by decision to keep manual call-site scheduling instead of introducing a `run_on_io` helper.

- [x] **4. Coroutine call sites** (8 methods: `CreateTask`, `AddNewJob`, `PilotClaimJob`, `ValidateTaskToken`, `UpdateJobStatus`, `RegisterNewPilot`, `DeleteHeartBeat`, `AddTaskDependency`): at each `co_await RunQuery` / `co_await BulkWrite` call, dispatch onto `m_io_thread_pool.get_scheduler()` and restore via `continues_on`:
   ```cpp
   auto compute_sched = co_await stdexec::read_env(stdexec::get_scheduler);
   auto result = co_await (m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(), q)
     | stdexec::continues_on(compute_sched));
   ```

- [x] **5. `ClearTask`** (uses `stdexec::when_all` with two parallel DB calls): capture `compute_sched` before the `when_all`, then pipe `continues_on(compute_sched)` after it:
   ```cpp
   auto compute_sched = co_await stdexec::read_env(stdexec::get_scheduler);
   co_await (stdexec::when_all(
       m_frontDB->RunQuery(m_io_thread_pool.get_scheduler(), q1),
       m_backDB->RunQuery(m_io_thread_pool.get_scheduler(), q2))
     | stdexec::continues_on(compute_sched));
   ```

- [x] **6. `main.cpp`**: pass `nIOThreads` when constructing `Director` (or call a `SetIOThreads(n)` setter, consistent with the existing `SetFrontDB`/`SetBackDB` pattern).

- [x] **7. Rename** `m_thread_pool` → `m_compute_pool` in `Server` for clarity now that two pools exist.

### Sizing guidance

| Pool | Thread count | Rationale |
|---|---|---|
| Compute | `hardware_concurrency` | CPU-bound work; more threads add contention |
| I/O | `= mongocxx::pool max_size` | Each I/O thread may hold one DB connection; surplus threads just queue |

Both values exposed as `OrchestratorConfig` fields with the defaults above.

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

- [x] **1.** Re-run clang-tidy with the command above to confirm the current finding list.
- [x] **2.** Apply fixes with `-fix` flag (or manually), excluding `modernize-use-trailing-return-type`.
- [x] **3.** Build and verify no regressions.

---

## Test coverage expansion

**Historical baseline (before this work):** 124 assertions across 9 test cases. Coverage was limited to the DB query builders, `Harness`, `MongoDBBackend` helpers, and `Utils`, with no orchestrator-layer coverage.

### Prerequisite refactor: extract `IDirector` interface

Extract `IDirector` as an abstract base class in `Director.h` (or a dedicated `IDirector.h`) with all public `Director` methods as pure virtuals. `Director` implements `IDirector`. `Server` holds `std::shared_ptr<IDirector>`.

**Rationale**: consistent with the existing `Backend` / `MockBackend` pattern (already a virtual interface mocked via `trompeloeil::mock_interface<Backend>`). Keeps `Server` a concrete class (avoids forcing its substantial `.cpp` implementation into headers). The virtual dispatch cost is irrelevant for a service object called once per request. An explicit interface will also make the future async refactor easier — changing what Director methods return (e.g. to senders) is a single, well-scoped change.

`MockDirector` will use `trompeloeil::mock_interface<IDirector>`, mirroring the existing `MockBackend`.

**Status: [x] Complete**

### [x] Step 1 — `Task` struct (pure logic, no deps)

New file: `testsuite/orchestrator/testTask.cpp`

All four predicates tested across boundary conditions:
- `IsActive()`: zero jobs, jobs present + readyForScheduling, readyForScheduling=false, all done/failed
- `IsFinished()`: totJobs=0 (false), all Done, partial Done
- `IsExhausted()`: pending=0 and error=0 (true), pending > 0 (false)
- `IsFailed()`: exhausted + failed > 0 (true), exhausted + failed = 0 (false)

### [x] Step 2 — `EnumArray` and `ts_queue`

New files: `testsuite/common/testEnumArray.cpp`, `testsuite/common/testQueue.cpp`

- `EnumArray`: enum-keyed access, default construction, value assignment, out-of-bounds check
- `ts_queue`: push/pop, empty(), size(), `consume_all()` drains queue correctly, concurrent pushes from N threads (all items recovered)

### [x] Step 3 — `toUserCommand` and `toPilotCommand`

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

### [x] Step 4 — `Director` with mocked Harness

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

### [x] Step 5 — `Server::HandleCommand` with `MockDirector`

Depends on the `IDirector` prerequisite refactor.

New file: `testsuite/orchestrator/testHandleCommand.cpp`

One scenario per variant arm for both `HandleCommand(UserCommand&&)` and `HandleCommand(PilotCommand&&)`. Key cases:
- `SubmitJob`: SHA256 hash is injected into the job and appears in the reply
- `ClearTask`/`CleanTask`: token validation failure short-circuits (Director never called)
- `InvalidCommand`: returns the `errorMessage` field verbatim
- `LivenessProbe`: returns `"OK"`, Director never called

### [x] Step 6 — `message_handler` / `pilot_handler` sender chains

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

---

## Pilot networking refactor: `Connection` hardening + coroutine/sender migration

**Goal**: replace the current fragile sync request/reply wrapper with a robust async transport that is safe under disconnects, shutdown, and concurrent traffic, while fitting the codebase's C++23 + `stdexec` direction.

### Why this is needed (current risks)

- `cv.wait(...)` without predicates/timeouts can block forever (connect/close paths).
- callback writes to a single `std::promise` can throw or race (`set_value` / `set_exception` paths).
- `send(..., ec)` ignores `ec`, then waits indefinitely for a reply.
- handlers capture `this` while `Connection` is movable; lifetime and callback ownership are unsafe.
- `Reconnect()` calls `endpoint.reset()` on a shared endpoint (can affect other connections).
- request/reply correlation is implicit (single in-flight message); unsolicited/out-of-order frames can break semantics.

### Target architecture

- Introduce a stable **transport state object** (`shared_ptr`/PIMPL style) that owns websocket callbacks and outlives transient wrappers.
- Build explicit **connection state machine** (`idle -> connecting -> open -> closing -> closed -> failed`) with guarded transitions.
- Replace ad-hoc single in-flight promise handling with an explicit **single-flight request slot** per connection and bounded lifecycle rules.
- Expose both:
  - coroutine API: `stdexec::task<ErrorOr<std::string>> SendAsync(...)`
  - sender API: `stdexec::sender auto SendSender(...)`
- Keep a compatibility sync API temporarily (`Send`) implemented as a thin adapter over async semantics.

### Refactor plan (phased)

#### Phase 0 — Contract and boundaries

- [x] Define transport contract (`connect`, `close`, `send`, timeout, cancellation, retry behavior).
  - **Timeout policy**:
    - default timeout is **60s** for bounded operations (`connect` and `close`);
    - `send` has **no timeout** and waits for server reply, unless interrupted by disconnect/shutdown/cancellation.
  - **Return model**:
    - sync compatibility methods return `ErrorOr<...>` (`Send` returns reply payload, `connect`/`close` return status/error).
    - async/coroutine and sender APIs wrap the same payload/error model (`task<ErrorOr<...>>` and sender equivalent).
  - **Cancellation policy**: operations honor shutdown/stop requests and complete with explicit cancellation error (no silent success).
  - **Retry policy**:
    - pilot keeps retrying connectivity while it is still allowed to run work;
    - concretely, retries continue while work is active and stop conditions are not met (e.g. `--maxJobs` not reached and `--maxTime` not exceeded);
    - once shutdown/stop is requested or run limits are reached, retries stop and pending operations complete with explicit error.
- [x] Decide ownership model for Worker/HeartBeat (`shared` single connection vs dedicated connections).
  - Use **dedicated connections**:
    - **control connection** for claim/update/sync request-reply traffic;
    - **heartbeat connection** for liveness heartbeats only.
  - **Rationale**: heartbeat traffic must not be blocked behind slow server replies on claim/update paths.
  - Reconnect and shutdown rules from the transport contract apply independently to both connections.
- [x] Decide reply correlation format (single-flight per connection, no request-id map).
  - **Single-flight contract**: each connection allows exactly one outstanding request at a time.
  - The next valid reply frame on that same connection is treated as the reply to the in-flight request.
  - Frames received when no request is pending are treated as unsolicited/protocol-error according to routing policy.
  - In-flight state is always cleared on disconnect, reconnect, shutdown, or cancellation.
- [x] Write migration ADR in code comments / design note section in TODO (this section is the seed).

##### Phase 0 design note (authoritative transport spec)

- This spec applies independently to both pilot connections:
  - **control connection** (claim/update/sync);
  - **heartbeat connection** (heartbeat traffic only).
- Default timeout for bounded operations (`connect`, `close`) is **60s** unless a callsite explicitly overrides it.

**State machine**

- States: `idle`, `connecting`, `open`, `closing`, `closed`, `failed`.
- Allowed transitions:
  - `idle -> connecting` (start connect)
  - `connecting -> open` (open callback)
  - `connecting -> failed` (fail callback / timeout / cancellation)
  - `open -> closing` (start close)
  - `open -> failed` (transport failure)
  - `closing -> closed` (close callback)
  - `closing -> failed` (close timeout / failure)
  - `failed -> connecting` (retry allowed)
  - `closed -> connecting` (reconnect allowed)

**API behavior by state**

- `Connect`:
  - valid from `idle`, `closed`, `failed`;
  - from `connecting`: no-op success (do not start a second concurrent connect);
  - from `open`: no-op success;
  - must complete within timeout (success or explicit error).
- `Send`:
  - valid only in `open`;
  - **single-flight rule**: only one in-flight request per connection; if violated by callers, return explicit busy/protocol error (do not create multiple in-flight sends on one connection);
  - completes with reply payload, disconnect error, shutdown error, or cancellation error;
  - if a frame arrives while no request is in flight, classify as unsolicited/protocol-error per routing policy.
- `Close`:
  - from `open`/`connecting`: transition to `closing`, complete within timeout;
  - from `idle`/`closed`/`failed`: no-op success;
  - any in-flight `Send` is completed with explicit error during close/shutdown.
- `Retry`:
  - only from `failed`/`closed`;
  - allowed while pilot is still eligible to run work (`--maxJobs` not reached, `--maxTime` not exceeded, and no stop/shutdown request);
  - disabled once stop conditions are met.
- `Shutdown`:
  - stop accepting new sends/connect attempts;
  - complete pending operations with explicit cancellation/shutdown error;
  - drive state to `closed` in bounded time.

#### Phase 1 — Safety stabilization of existing code (no behavior expansion)

- [x] Replace all unconditional waits with predicate waits; keep `connect`/`close`/shutdown waits bounded by timeout. (`Connection.cpp:51,81` — both use `cv.wait_for` with predicates and 60s timeouts)
- [x] Handle `send` error_code immediately and return `ErrorOr` failure. (`Connection.cpp:164-167`)
- [x] Make callback completion idempotent (never throw on already-satisfied completion paths). (`MessageReply::TryComplete*` guards on `m_request_state` at `Connection.cpp:197,208,220`)
- [x] Remove/forbid move semantics for `Connection` unless backed by stable shared state. (`Connection.h:27` — move ctor/dtor deleted)
- [x] Stop using endpoint-wide `reset()` from per-connection logic. (No `reset()` calls anywhere in pilot code)
- [x] Ensure destructor/shutdown is bounded and cannot deadlock joins. (`Close()` has 60s timeout + stop-token; `~Client()` uses `stop_perpetual()` before `join()`)

> **Fixed**: `Client.cpp` — `get_status()` / `SocketState` replaced with `state()` / `State::Closing` / `State::Closed`.

#### Phase 2 — New async transport core

- [x] Add transport with explicit state machine + mutex discipline. (Embedded in `Connection` — no separate `PilotTransport` class. State machine + `m_state_mutex` guards transitions. `Connection.h:21,59-62`)
- [x] Add async connect/close with timeout and stop-token cancellation. (`Connect()`: `cv.wait_for` with 60s timeout + `m_stop_token.stop_requested()` predicate at `Connection.cpp:51-53`; `Close()`: same pattern at `Connection.cpp:81-84`)
- [x] Add async send with:
  - [x] single in-flight request slot per connection (single-flight contract)
  - [x] disconnect handling (fail the in-flight send explicitly via `TryCompleteError()` in `on_fail`/`on_close`)
  - [x] cancellation/shutdown cleanup — `Send()` checks `m_stop_token.stop_requested()` before blocking; returns cancellation error. Also uses `wait_for(10min)` instead of unbounded `get()`. (`Connection.cpp:182-192`)
- [x] Add server-message routing policy (reply vs unsolicited/event frames) — `on_message` logs unsolicited messages at trace level instead of silently dropping them. (`Connection.cpp:143-146`)

#### Phase 3 — Sender/coroutine integration

> **Design decision**: No separate `PilotTransport` class. The state machine is embedded directly in `Connection` (see Phase 2 above). This keeps the transport tight with the connection lifecycle and avoids an extra indirection layer.

- [x] Shared `PMS::Async<T>` (stdexec::task<T>) extracted to `common/Async.h` for use by both orchestrator and pilot.
- [x] Provide sender-first API (`SendSender`) and task wrapper (`SendAsync`), aligned with orchestrator `stdexec` usage. (`Connection::SenderSend()` at `Connection.cpp:206`, `Connection::AsyncSend()` at `Connection.cpp:249`)
- [x] Sync compatibility API renamed to `SyncSend()` (was `Send()`), async stub added as `AsyncSend()`. (`Connection::SyncSend()` delegates to `SenderSend` via `stdexec::sync_wait`; `Connection::AsyncSend()` returns via `co_return co_await SenderSend(message)`; `SenderSend` is the single canonical implementation)
- [x] Add scheduler handoff policy (`on(...)`) so websocket callbacks stay lightweight. (`on_message` dispatches `TryCompleteSuccess` through `exec::start_detached(stdexec::on(m_thread_pool.get_scheduler(), ...))` at `Connection.cpp:150-157`)
- [x] Fixed test hang: added `m_has_real_connection` flag to prevent `Close()` wait in destructor for `no_connect` connections; added null connection guard in `SenderSend()`; reduced thread pool size from 4 to 2 to fix flaky test behavior

#### Phase 4 — Call-site migration

> **Completed state**: `Worker` and `HeartBeat` migrated to use `std::stop_token` for cancellation. `SyncSend` delegates to `SenderSend`.

> - `Worker::MainLoop()` — uses `std::stop_token` for cancellation, calls `SyncSend()` for blocking claim job path
> - `Worker::SendJobUpdates()` — uses `std::stop_token`, dedicated thread with greedy drain behavior on exit
> - `HeartBeat::updateHB()` — converted to coroutine using `AsyncSend()`, stop token drives exit loop
> - `HeartBeat::run_heartbeat()` — wrapper function for `std::jthread` to run the coroutine via `stdexec::sync_wait()`
> - `Worker::Kill()` — sets stop token, joins both threads (update thread drains first), then terminates job process
> - `Client::PersistentConnection()` — unchanged, retry loop handles connection establishment
> - `Connection::SenderSend()` — single canonical send pipeline (auto return type, defined before SyncSend/AsyncSend)
> - `Connection::SyncSend()` — delegates to `stdexec::sync_wait(SenderSend(message))`
> - `Connection::AsyncSend()` — delegates to `co_await SenderSend(message)`

- [x] Migrate `Worker` claim/update paths to async API (`Worker.cpp:148-413`).
- [x] Migrate `HeartBeat` loop to async send + cancellation-aware sleep loop (`HeartBeat.cpp:17-40`).
- [x] Replace ad-hoc shutdown signaling (`std::promise<void>`) with unified `std::stop_token` flow (`Worker.h:69-70`, `HeartBeat.h:31`).
- [x] `SendJobUpdates()` drains queue on stop token before exiting (`Worker.cpp:132-146`).

#### Phase 5 — Legacy removal and cleanup

> **Current legacy in `Connection`**: `m_sendMutex` (still protects SyncSend single-flight via sender discipline), `m_connection_result` + `cv`/`cv_m` (used by SenderSend's future-based reply wait), `m_promise_mutex` (MessageReply internals). These can be removed once sync callers are migrated to async/sender APIs.
>
> **Design decision: SyncSend callers are valid blocking paths.** The three call sites that use `SyncSend` (Worker::Register, Worker::MainLoop claim, Worker::SendJobUpdates) are inherently sequential — each waits for a server reply before proceeding to the next step. Converting these to async/sender APIs would add complexity (coroutine machinery, callback chains, state tracking) without improving throughput or responsiveness. The worker's execution model is a blocking request → process → request loop, so keeping these paths as blocking adapters over `SenderSend` via `stdexec::sync_wait` is the right tradeoff. `SyncSend` stays as the canonical blocking adapter, not a deprecated pathway.

> **Completed**: `SyncSend` refactored to delegate to `SenderSend` via `stdexec::sync_wait` (`Connection.cpp:209-222`). `SenderSend` is now the single canonical send pipeline implementation. `AsyncSend` delegates to `SenderSend` via `co_await`. All call sites (Worker::Register, Worker::MainLoop claim, Worker::SendJobUpdates) work through `SyncSend` as the blocking adapter. `FMT_HEADER_ONLY` added to PMSPilotLib to fix spdlog v1.16.0 linker error.

- [ ] Keep `SyncSend` as canonical blocking adapter (see Phase 5 design decision above).
- [ ] Remove dead fields/mutexes/condition variables from `Connection` (`cv`/`cv_m`, `m_connection_result` — only used by legacy sync `Send()` pre-refactor).
- [ ] Tighten logging taxonomy (connect/reconnect, timeout, protocol, shutdown).

### Verification plan

- [ ] Unit tests: state-machine transitions, `connect`/`close` timeout behavior, disconnect handling, duplicate/late replies.
- [x] Manual verification: no unbounded waits — all `cv.wait()` replaced with `cv.wait_for(predicate, timeout)` or stop-token escape.
- [x] Manual verification: `Send()` error_code checked immediately (`Connection.cpp:164-167`).
- [x] Manual verification: callbacks are idempotent (`TryComplete*` guard on `m_request_state`).
- [x] Unit tests: single in-flight slot cleanup on success/failure/disconnect/cancel. (`testMessageReply.cpp` + `testConnection.cpp` — 16+9=25 pilot tests covering Activate → TryComplete* paths, idempotency, idle/no-op, double-cleanup safety)
- [ ] Integration tests: reconnect under flaky network, serialized single-flight sends per connection, graceful shutdown during traffic.
- [ ] Regression tests for Worker + HeartBeat end-to-end behavior.

> **Fixed**: `Client.cpp` — `get_status()` / `SocketState` replaced with `state()` / `State::Closing` / `State::Closed`.

### Acceptance criteria

- No unbounded wait in connect/close/shutdown paths.
- `Send` has no timeout by design, but always terminates with reply or explicit error on disconnect/shutdown/cancellation.
- No callback-thrown exceptions escaping websocket handlers.
- Pending async sends are always completed (value or explicit error) on disconnect/shutdown/cancellation.
- Worker and HeartBeat can run/stop repeatedly without deadlocks or leaked background activity.
- New transport is used by pilot runtime; legacy sync wrapper is removed, but `SyncSend` is retained as the canonical blocking adapter for inherently sequential call sites.

### [Resolved] HeartBeat destructor called before `p_updateHeartBeat` could be sent

**Symptom**: Orchestrator received `p_registerNewPilot` and `p_deleteHeartBeat` but never `p_updateHeartBeat`.

**Root causes identified and fixed**:
1. **Thread pool starvation deadlock** — both `MainLoop` and `SendJobUpdates` workers blocked in `SenderSend::wait_for`, unable to dispatch `TryCompleteSuccess` callbacks. Fixed: `on_message` now calls `TryCompleteSuccess` directly on the asio callback thread (Phase 5 change to `Connection.cpp`).
2. **10-minute unbounded `SenderSend` timeout** — blocking `sync_wait` on a 10-minute timeout meant stop_token changes were never observed. Fixed: `SenderSend` timeout is now stop_token-aware with 5-second polling (10-minute total).
3. **`HeartBeat` using `std::jthread`** — stop_token passed through constructor created contention. Fixed: `HeartBeat` now uses `std::thread` and gets stop_token internally from member `m_stop_source`.
4. **Race between thread startup and stop signal** — added `std::latch` synchronization in `Worker::Start()` to ensure both threads are ready before `Start()` returns.

**Verification**: `p_updateHeartBeat` now successfully sent by pilot and received by orchestrator. Backtrace confirmed destructor called at `Worker.cpp:424` (end of `MainLoop`), after the first heartbeat was already transmitted.

### [Resolved] `HeartBeat::IsAlive()` was always false, causing premature pilot exits on transient disconnects

**Symptom**: pilots exited after temporary send failures because `Worker::MainLoop` treats `!hb->IsAlive()` as a terminal condition.

**Root causes identified and fixed**:
1. **`m_alive` was never updated** — `HeartBeat` initialized `m_alive` to false and never set it true.
2. **No bounded liveness hysteresis** — a single failed heartbeat attempt could immediately be interpreted as dead transport.
3. **Stale pilot entries after reconnect churn** — `RegisterNewPilot` inserted pilots without `lastHeartBeat`, so `UpdateDeadPilots` (which matches `lastHeartBeat < threshold`) could not collect those rows.

**Fixes applied**:
1. `HeartBeat` now tracks liveness with atomics: successful heartbeat replies reset failure count and set `m_alive=true`.
2. Consecutive heartbeat failures are counted and only mark `m_alive=false` after a small threshold (3), reducing flapping on transient network issues.
3. `Director::RegisterNewPilot` initializes `lastHeartBeat` at insert time so dead-pilot cleanup can remove stale registrations even when a pilot dies before its first heartbeat update.

---

## Atomic asynchronous job claims (RC8)

**Goal**: remove claim polling and compute-worker blocking while preserving atomic job assignment semantics.

### Implementation

- [x] `PilotClaimJob` performs one asynchronous `FindOneAndUpdate` per claim on the I/O scheduler.
- [x] Match the existing eligible statuses, active tasks, and tag semantics.
- [x] Atomically set `Claimed`, `pilotUuid`, increment retries, and update `lastUpdate`.
- [x] Return the pre-update job document; return `{"sleep": true}` for no matches or recoverable DB failures.
- [x] Remove the prefetch queue, polling loops, claim coordinator thread, and claimed-job bookkeeping.

### Verification

- [x] Director unit tests cover query shape, task/status/tag filters, successful claims, no-match sleep, and DB failure.
- [x] Add an opt-in MongoDB integration test for concurrent claims and pre-update return behavior. Run it with
  `PMS_TEST_MONGODB_HOST` and, optionally, `PMS_TEST_MONGODB_DB` against a dedicated test database.
- [x] Full debug test suite: 122 passed, 1 opt-in integration test skipped, 967 assertions passed.
- [ ] Load-test 50 and 500 concurrent claims against a dedicated deployment and measure MongoDB pressure, CPU, and
  liveness responsiveness.
- [ ] Add an explicit claim concurrency limit if shared I/O scheduling proves unfair under load.

### Follow-ups: heartbeat recovery

- [ ] Add an optional send-operation timeout to the connection sender API. Retain unlimited waiting as the default
  for existing control paths.
- [ ] Configure heartbeat sends with a 30-second timeout, while allowing tests to inject shorter durations.
- [ ] On timeout or transport failure, clear the outstanding reply slot, close or normalize the failed connection,
  and reconnect before the next attempt. A late reply must not complete a newer request.
- [ ] Reset the failure counter after a successful heartbeat. Preserve the existing three-consecutive-failure
  threshold, and make stop requests interrupt waits promptly.

### Independent follow-ups

- [ ] Investigate the independent stdexec idle-spin CPU cost.
- [ ] Add explicit unknown-pilot/re-registration handling for empty pilot queries.
# Historical investigation checkpoint (pre-RC8)

The claim-polling remediation described below was superseded by the RC8 atomic asynchronous claim implementation
documented above. Retain this section as incident history and evidence; do not reintroduce its coordinator or polling
architecture.

**Symptom**: Under zero-pending-jobs conditions, `PilotClaimJob` blocks compute pool threads in `sleep_for(50ms)` polling loops. This creates a self-reinforcing thread starvation cascade: pilots get `{"sleep":true}` → immediately call `p_claimJob` again → new blocking loop → compute pool fills → remaining threads can't handle reconnects → EOF disconnects → more pilots → more blocked threads → pool exhaustion.

**Root cause**: `PilotClaimJob` (`Director.cpp:92-99`) unconditionally pushes the pilot to `m_claimRequests` and then enters two blocking `while` loops with `sleep_for(50ms)`, waiting for `RunClaimQueries` to process the queue. When pending jobs exist, `RunClaimQueries` finds them quickly and the loops exit within ~50-500ms. When **no** pending jobs exist, the loops still block compute threads for ~100ms each (the time until `RunClaimQueries` queries the DB, finds nothing, and sets `claimed=true, job={"sleep":true}`). With a hardcoded pool of 32 threads, even a few dozen pilots all simultaneously blocked creates thread starvation.

**Evidence**: During the reconnect herd at 11:55, 3,962 EOF disconnects occurred in 2 hours. Each disconnect causes pilot re-registration + immediate `p_claimJob` call → blocking loop. The main thread was observed in `futex_wait_queue` wchan, consistent with compute pool being saturated with blocked pilots.

**Fix plan**: Before entering the blocking polling loop, query the DB to check if there are pending jobs for the pilot's **specific tasks only**. If no pending jobs exist for any of the pilot's tasks, return `{"sleep":true}` immediately without pushing to `m_claimRequests` and without entering the polling loops. This frees the compute thread instantly. Only proceed with the blocking loop if pending jobs actually exist for this pilot.

### Steps

1. **`Director.cpp::PilotClaimJob`**: After the task exhaustion check (line 86), add a DB query to check for pending jobs for this pilot's tasks:
    - Build the same `matches` filter used by `RunClaimQueries` (status IN {Pending, Error, OutboundTransferError, InboundTransferError} + task IN {pilot's active tasks} + tags matching)
    - Use `m_frontDB->RunQuery()` with `limit=1` (we only need to know if at least one job exists)
    - Wrap DB dispatch with `continues_on(compute_sched)` to resume on the compute pool

2. **Decision logic**:
    - If query returns empty → `co_return R"({"sleep": true})"_json` immediately (no blocking loop, no thread consumption)
    - If query returns results → proceed with existing `m_claimRequests.push()` + polling loop (jobs exist, will be found quickly by `RunClaimQueries`)

3. **Key detail — per-pilot task filtering**: The DB query must use the pilot's specific active tasks (`m_tasks` filtered by `IsActive()`) as the `task IN (...)` clause. This ensures we only return sleep if there are genuinely no jobs for **this pilot's** tasks. A pilot with task A should not be put to sleep if task B (not assigned to this pilot) has pending jobs.

4. **Consideration — duplicate query with `RunClaimQueries`**: This adds one extra DB query per claim attempt. Tradeoff analysis:
    - **Before fix**: 0 extra DB queries, but each claim attempt blocks a compute thread for ~100ms (wasted during zero-jobs periods)
    - **After fix**: 1 extra DB query per claim attempt, but compute thread is freed instantly during zero-jobs periods
    - During normal operation (jobs available), the pilot gets a job quickly and the query is a one-time cost per claim cycle
    - During zero-jobs operation (the problematic case), the query prevents thread starvation entirely
    - `RunClaimQueries` can also be optimized: skip DB queries for pilots that already have cached job pools (existing behavior at line 158), reducing total query count

5. **Optional enhancement for `RunClaimQueries`**: When `RunClaimQueries` sets a pilot to sleep (line 215-218), mark the pilot with a timestamp (`m_lastSleepTime[pilotUuid]`) and skip re-querying the DB for this pilot until a minimum interval has elapsed (e.g., 5-10 seconds). This reduces redundant DB queries for pilots that have been sleeping.

### Verification

- Build and run `run_tests` (update `testDirector.cpp` expectations for the new query path)
- Load test: simulate zero-pending-jobs + high pilot claim rate (50+ concurrent pilots), verify:
    - Compute pool threads are not blocked in `sleep_for` loops
    - No thread starvation — main thread not stuck in futex
    - CPU stays well below saturation during zero-jobs periods
- Load test: simulate normal operation with pending jobs, verify:
    - Pilots still get jobs promptly
    - No regression in claim latency

---

# Native HTTP health endpoint

**Goal**: let Kubernetes probe the orchestrator cleanly without adding a separate HTTP server.

- [x] Register websocketpp's native HTTP handler on the existing endpoints.
- [x] Return HTTP 200 with body `OK` for `/healthz` and HTTP 404 for unrelated paths.
- [x] Add black-box integration tests using plain HTTP requests against a live `Server`.
- [x] Replace Kubernetes TCP probes with HTTP liveness/readiness probes on the user port.

---

# Investigation on orchestrator slowdown

Checkpoint #1
----------------------------------------
<overview>
The user asked for ongoing production debugging of PMSOrchestrator in Kubernetes: identify why CPU/memory spiked, why pilots disconnected, and validate fixes over time. I investigated by correlating orchestrator logs, ingress logs, live pod/process/socket state, and MongoDB frontend/backend data, then implemented and deployed code fixes (via commit/push) for confirmed pilot liveness and stale-pilot cleanup issues. The approach was evidence-first: reproduce patterns, map to code paths, patch root causes we could verify, then repeatedly monitor real cluster behavior.
</overview>

<history>
1. User asked to read `AGENTS.md` and `TODO.md` and report familiarity
   - Read both files (chunked for large TODO).
   - Reported project architecture, build/test conventions, and active TODO status.

2. User reported production incident (CPU 100%, memory growth, pilot disconnects) and asked to understand cause
   - Collected pod/deployment/top/events, exported orchestrator logs (72h), analyzed frequency patterns.
   - Found heavy `EOF` websocket errors and high reconnect/re-register churn.
   - Identified ingress timeout annotations at 3600s and correlation with `/pilot` close durations.
   - Inspected runtime process/thread/socket stats and DB contents.

3. User asked for simpler explanation
   - Explained likely chain: periodic connection cuts -> reconnection herd -> CPU spikes; stale pilot records driving memory growth.

4. User challenged assumption (“pilots should resume if already in DB”)
   - Traced pilot code path and found `HeartBeat::m_alive` initialized false and never set true.
   - Showed worker exits on send failures when `IsAlive()` is false, causing process restart and re-registration with new UUID.

5. User requested confirmation and fix proposal
   - Confirmed via code references.
   - Proposed fix: real heartbeat liveness tracking + stale row cleanup hardening.

6. User gave permission to fix
   - Implemented code changes in pilot heartbeat + orchestrator register path + test updates.
   - Built tests and ran targeted suites successfully.
   - Updated `AGENTS.md` and `TODO.md` housekeeping.
   - Committed and pushed to `upgrades` branch.

7. User requested ongoing monitoring at multiple checkpoints (after restart, +90m, overnight, later checks)
   - Repeatedly sampled pod health, CPU/RAM, log error rates, ingress close patterns, and DB pilot health.
   - Observed mixed periods:
     - At times stable (no reconnect storm, clean heartbeat state).
     - At times bursts reappeared (mass EOF + registrations), later not strictly 3600s.
   - Verified in RC6 that `lastHeartBeat` is present for all pilot docs (no missing heartbeat field accumulation).

8. User asked why no heartbeat messages appeared server-side in one run
   - Found backend tasks collection empty after restart; pilot registration returned zero valid tasks (`tasks: []`), pilots exited before heartbeat phase.

9. User asked about memory proportionality to active pilots
   - Measured process RSS, socket count, FD breakdown, and active pilot/connection counts.
   - Concluded memory scales with connections and runtime socket state more than just the in-memory pilot map.

10. User asked about max active connection limit
   - Checked app code and runtime limits.
   - Found no explicit app-level max; practical limits are ingress/kernel/resources.

11. User asked to identify hostnames for Done and Failed jobs
   - Queried DBs and joined jobs to pilots by UUID.
   - Reported Done and Failed distributions and noted expected `<missing>` hostnames for some old UUIDs (live-pilot table semantics).

12. User asked whether latest spike was new pilots vs re-registrations
   - Quantified last-hour registration composition:
     - Mostly registrations on previously seen hostnames (re-spawn/re-register events), plus some truly new hosts.
     - No repeated UUIDs in log slice (new UUID each registration).
</history>

<work_done>
Files updated and committed:
- `src/pilot/HeartBeat.h`
  - Added atomic liveness/failure state (`m_alive`, consecutive failure counter, threshold).
- `src/pilot/HeartBeat.cpp`
  - Heartbeat liveness now set true on successful `"Ok"` reply.
  - Consecutive failures tracked; mark not alive after threshold.
  - Added handling for unexpected heartbeat replies.
- `src/orchestrator/Director.cpp`
  - `RegisterNewPilot` now initializes `lastHeartBeat` at insert time.
- `testsuite/orchestrator/testDirector.cpp`
  - Updated test expectation to require `lastHeartBeat` in register insert doc.
- `AGENTS.md`
  - Updated status note for pilot networking refactor to include liveness/cleanup improvement.
- `TODO.md`
  - Added resolved note for `IsAlive()` always-false issue and applied fixes.

Git actions:
- Commit: `09d122c` (`Fix heartbeat liveness and pilot cleanup`)
- Pushed to: `origin/upgrades`

Validation performed:
- Built `run_tests` target successfully.
- Ran targeted tests:
  - `[pilot][HeartBeat]` passed.
  - `[Director]` passed.

Current state:
- RC6 deployment often stable, with clean pilot heartbeat metadata (`withoutHeartbeat=0`, `staleOver1h=0`).
- Intermittent reconnect herd events still occur at times, causing transient CPU spikes and re-registration bursts.
- Original stale-pilot-field problem is fixed in RC6 behavior.
</work_done>

<technical_details>
- Root-cause findings (confirmed):
  - Pilot bug: `HeartBeat::m_alive` was never set true; worker interpreted transient failures as dead heartbeat and exited, causing churn.
  - Frontend pilot cleanup gap: rows inserted without `lastHeartBeat` were not removable by dead-pilot query (`lastHeartBeat < threshold`), causing accumulation.
- Post-fix behavior:
  - Heartbeat liveness now reflects real send outcomes.
  - Pilot rows now always carry `lastHeartBeat` from registration.
- Still-observed production behavior:
  - Reconnect herds can still happen (sometimes no longer strict 3600s), likely triggered by external/network/client-wave events.
  - During herd windows, orchestrator CPU spikes due to connection churn, message parsing/logging, register/claim/update flow.
- Architectural pressure points:
  - `pilot_handler` dispatches every pilot message via detached work on compute pool.
  - `PilotClaimJob` coroutine contains blocking polling loops (`sleep_for(50ms)` while waiting on shared map state), which can tie up compute workers under surge.
  - Shared state (`m_claimedJobs`, `m_activePilots`, parts of `m_tasks`) is accessed from multiple threads/coroutines with limited synchronization, representing concurrency risk under load.
- Runtime environment observations:
  - Ingress annotations include websocket proxy timeouts at 3600s.
  - At various times ingress request durations clustered near 3600s; in later events some closes were much longer-lived.
  - Process memory snapshots showed large socket/FD footprint; memory scaling aligns with active connections + runtime buffers, not only pilot metadata map.
- Open/uncertain:
  - Exact trigger for large synchronized long-lived disconnect waves remains unproven (likely external/network/ingress/client wave).
  - Need deeper concurrency/load profiling to isolate primary in-process bottleneck during herds.
</technical_details>

<important_files>
- `src/pilot/HeartBeat.h`
  - Central to liveness semantics used by worker shutdown logic.
  - Changed to atomic liveness and failure-threshold fields.
  - Key area: class fields and `IsAlive()`.

- `src/pilot/HeartBeat.cpp`
  - Core heartbeat loop behavior.
  - Changed to set/reset liveness based on actual heartbeat replies and failure streak.
  - Key area: `updateHB()` loop.

- `src/pilot/Worker.cpp`
  - Contains branch that exits worker when send fails and heartbeat is not alive.
  - Important for understanding re-register churn and pilot restarts.
  - Key area: `MainLoop()` error path around `hb->IsAlive()`.

- `src/orchestrator/Director.cpp`
  - Register path and claim/update flow under high load.
  - Changed `RegisterNewPilot` to set `lastHeartBeat`.
  - Important hotspots:
    - `RegisterNewPilot()`
    - `PilotClaimJob()` polling loops
    - `RunClaimQueries()` claim batching/shared state logic.

- `src/orchestrator/Server.cpp`
  - Pilot message handling dispatch model (`start_detached` on compute pool).
  - Important for surge behavior under many concurrent pilot messages.
  - Key area: `pilot_handler()` and `MakePilotReplySender()`.

- `testsuite/orchestrator/testDirector.cpp`
  - Updated to enforce new `lastHeartBeat` registration behavior.
  - Confirms change contract.

- `AGENTS.md`, `TODO.md`
  - Housekeeping and project state tracking; updated per repository convention.
</important_files>

<next_steps>
Completed root-cause analysis. Dominant bottleneck identified: `PilotClaimJob` blocking loops consume compute threads when no pending jobs exist, causing thread starvation cascade during reconnect herds.

1. **[In TODO above]** Implement the `PilotClaimJob` preemptive check: query DB for pending jobs per pilot's tasks before entering the blocking loop; return `{"sleep":true}` immediately if none exist.
2. Reduce trace log volume (heartbeat logging → debug level, throttle per-pilot, conditional DB query logging in `RunClaimQueries`).
3. Add `std::mutex` guards to `m_activePilots`, `m_claimedJobs`, `m_tasks` to eliminate undefined behavior from concurrent unsynchronized access.
4. Fix compute pool size (respect config or set explicit value — currently hardcoded to 32 ignoring constructor parameter).
5. Run load tests: zero-jobs + high pilot count (verify no thread starvation), normal operation with pending jobs (verify no regression).

Most recent in-progress activity:
- Completed root-cause analysis correlating live cluster data with code paths.
- Drafted fix plan for `PilotClaimJob` blocking starvation (added to TODO above).
</next_steps>
