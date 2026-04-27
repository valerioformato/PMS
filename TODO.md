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
