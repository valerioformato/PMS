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

- [ ] Provide sender-first API (`SendSender`) and task wrapper (`SendAsync`), aligned with orchestrator `stdexec` usage.
- [ ] Implement sync compatibility shim only where still needed. (`Connection::Send()` is currently the primary API; no shim needed yet since nothing calls it through an adapter)
- [ ] Add scheduler handoff policy (`on(...)` / `continues_on(...)`) so websocket callbacks stay lightweight. (`on_message` currently calls `TryCompleteSuccess` directly on the websocketpp thread — no scheduler dispatch)

#### Phase 4 — Call-site migration

> **Current state**: `Worker` and `HeartBeat` both use the synchronous `Connection::Send()` API. No async API exists yet (Phase 3).
>
> - `Worker::MainLoop()` calls `m_wsConnection->Send()` for `p_claimJob` (line 179) and `m_wsClient->PersistentConnection()` to create a separate connection for `HeartBeat` (line 139).
> - `HeartBeat::updateHB()` calls `m_wsConnection->Send()` in a 15s polling loop (line 44), with `std::future<void>` as the exit signal instead of `std::stop_token`.
> - `Worker::SendJobUpdates()` calls `m_wsConnection->Send()` for status updates (line 116).
> - `Client::PersistentConnection()` has a retry loop with `sleep_for(5s)` that duplicates reconnect logic (lines 29-36).

- [ ] Migrate `Worker` claim/update paths to async API.
- [ ] Migrate `HeartBeat` loop to async send + cancellation-aware sleep loop.
- [ ] Remove polling/sleep retry loops that duplicate transport reconnect logic.
- [ ] Replace ad-hoc shutdown signaling (`std::promise<void>` in `HeartBeat`) with unified `std::stop_token` flow.

#### Phase 5 — Legacy removal and cleanup

> **Current legacy in `Connection`**: `m_sendMutex` (protects `Send` single-flight, replaceable by sender discipline), `m_connection_result` + `cv`/`cv_m` (used by connect/close/send sync waits), `m_promise_mutex` (MessageReply internals). These would be removed/consolidated once async API is the only path.

- [ ] Remove deprecated sync-only pathways once all users are migrated.
- [ ] Remove dead fields/mutexes/condition variables from `Connection` (`m_sendMutex`, `cv`/`cv_m`, `m_connection_result` — only used by sync `Send()`).
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
- New transport is used by pilot runtime; legacy sync wrapper is removed or isolated behind a temporary adapter.
