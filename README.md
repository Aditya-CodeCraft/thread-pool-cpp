# Thread Pool and Thread-Safe Task Queue (C++17)

A small, reusable C++17 thread pool built on a thread-safe blocking queue.
`submit()` accepts any callable plus arguments and returns a `std::future` that
delivers the result, or the exception the task threw. Shutdown is explicit or
automatic (RAII): it stops accepting work, **drains** every accepted task, and
joins every worker.

```cpp
#include "thread_pool.hpp"

tp::ThreadPool pool(4);
std::future<int> sum = pool.submit([](int a, int b) { return a + b; }, 2, 3);
std::cout << sum.get();  // 5
```

## Features

- Generic MPMC blocking queue (`tp::ThreadSafeQueue<T>`) with close/drain semantics; works with move-only types.
- Fixed-size pool (`tp::ThreadPool`) with a configurable number of workers (default: `hardware_concurrency()`, at least 1).
- Variadic `submit(f, args...)` that perfect-forwards into the task and returns `std::future<R>`, for any `R` including `void` and move-only types.
- Exceptions thrown by tasks travel to `future::get()`; worker threads never die from a task's exception.
- Work-helping `wait(future)`: a task waiting on its subtasks runs queued work instead of blocking, so nested parallelism can't starve the pool.
- Safe shutdown: idempotent, callable from several threads at once, drains the queue, joins all workers, and detects self-shutdown from a worker (throws instead of deadlocking).
- No busy waiting: idle workers sleep on a condition variable.
- 40 GoogleTest tests, including multi-producer stress and destruction under load.
- ASan+UBSan and TSan build configurations, and a GitHub Actions matrix (GCC and Clang × debug/release/asan-ubsan/tsan).

## Project layout

```
include/thread_safe_queue.hpp   header-only blocking queue (template)
include/thread_pool.hpp         ThreadPool class + submit() template
src/thread_pool.cpp             non-template parts: constructor, shutdown, worker loop
tests/                          GoogleTest suites (fetched with FetchContent, pinned + hashed)
examples/example.cpp            the README example, built as thread_pool_example
CMakePresets.json               debug / release / asan-ubsan / tsan
.github/workflows/ci.yml        CI matrix
```

## Architecture and design decisions

```
 producers ── submit() ──► [ ThreadSafeQueue<std::function<void()>> ] ──► N workers
     ▲                              (mutex + condition_variable)            │
     └──────────── std::future<R> ◄── std::packaged_task<R()> ◄─────────────┘
```

| Decision | Why |
|---|---|
| One shared queue, not per-worker queues | Simple and correct; good enough for coarse tasks. Work stealing is a possible extension (see limitations). |
| Unbounded queue | `submit()` never blocks the producer. The trade-off is that there's no back-pressure. |
| Queue holds `std::function<void()>` | Type erasure lets a single queue carry tasks of every return type. |
| `packaged_task` behind `shared_ptr` | `packaged_task` is move-only but `std::function` needs a copyable target; the `shared_ptr` makes the wrapper copyable. |
| Closing is a queue feature | `push()` and `close()` take the same mutex, so a task is either accepted (and will run) or rejected. There is no window where it's accepted and then dropped. |
| `PoolStoppedError` on submit after shutdown | Failing loudly beats returning a future that would never become ready. |
| Template code in headers, the rest in `src/` | Templates must be visible to callers; everything else is compiled once. |

## Thread synchronization

- **Queue state** (`deque` + `closed_` flag) is guarded by a single `std::mutex`. Every access goes through `std::lock_guard` or `std::unique_lock` (RAII), so a lock can't be leaked by an early return or an exception.
- **Waiting** uses `condition_variable::wait(lock, predicate)`. The predicate (`closed_ || !items_.empty()`) is re-checked under the lock, which makes spurious wake-ups harmless and prevents lost wake-ups: a push that happens before the wait is seen by the predicate.
- **Notification** happens *after* releasing the lock, so the woken thread doesn't immediately block on a mutex the notifier still holds. `push` uses `notify_one` (one item, one consumer). `close` uses `notify_all` (every consumer must wake up and exit).
- **`std::atomic<bool> stopping_`** backs `is_running()`, so callers can query the state lock-free. Correctness does *not* depend on this flag; it depends on the queue's closed state.
- **Shutdown** is serialised by `shutdown_mutex_`. A second concurrent caller blocks until the first has finished joining, so *every* `shutdown()` call returns only after all work is done.
- **Self-shutdown detection** compares the caller's thread id against `worker_ids_`, a vector written only in the constructor. It deliberately doesn't read `workers_`, because `join()` mutates those `std::thread` objects, and reading them during a concurrent shutdown would be a data race.

## How `submit()` works

```cpp
template <typename F, typename... Args>
auto submit(F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>>;
```

1. **Result type.** `std::invoke_result_t` computes what calling the *decayed* callable with *decayed* arguments returns. It works for lambdas, function pointers, functors and member pointers.
2. **Binding.** A lambda init-captures `std::forward<F>(f)` and `std::make_tuple(std::forward<Args>(args)...)`. Rvalues are moved in and lvalues are copied, exactly once. When the task runs, `std::apply` unpacks the tuple into the call.
   - Like `std::async`, arguments are stored by value. To share an object, pass `std::ref(obj)`. A callable that takes `T&` won't compile with a plain lvalue, which catches accidental dangling references at compile time.
3. **Packaging.** The bound lambda goes into a `std::packaged_task<R()>` held by a `shared_ptr`, and its `get_future()` is taken.
4. **Enqueue.** A `[task]{ (*task)(); }` wrapper is pushed into the queue. If the queue is closed, `submit` throws `PoolStoppedError`.

## Nested tasks and `wait()`

A fixed-size pool has a classic deadlock: if every worker runs a task that calls `future.get()` on a subtask that is still queued, no worker is left to run the subtasks. `ThreadPool::wait(future)` avoids this:

```cpp
auto parent = pool.submit([&pool] {
    auto child = pool.submit([] { return 20; });
    return pool.wait(child) + 1;   // child.get() here could deadlock
});
```

- **Off the pool's threads,** `wait(f)` is just `f.get()`.
- **On a worker,** while the result isn't ready it pops and runs other queued tasks (`try_pop`, never spinning). When the queue is empty, the awaited task has already been taken by some worker, which is either running it or helping the same way. In that case it is safe to block in `future.wait()`.
- Exceptions are rethrown exactly like `get()`.

The guarantee covers acyclic dependencies between tasks of the same pool. It can't help with futures from outside the pool, cyclic dependencies, or tasks that never return.

## Futures and packaged tasks

`std::packaged_task<R()>` couples a callable with a *shared state*, the same state a `std::future<R>` reads from:

- When a worker runs the task, the return value is stored in the shared state and the future becomes ready.
- If the task throws, `packaged_task::operator()` catches the exception and stores it as a `std::exception_ptr`. `future::get()` rethrows it in the caller's thread. That's why the worker loop needs no `try`/`catch`, and why a throwing task can't kill a worker.
- For `void` tasks, `future<void>::get()` simply waits (or rethrows).
- Every accepted task is run before the workers exit, so the pool never produces a "broken promise".

## Shutdown and resource management

`shutdown()` (also called by the destructor):

1. Marks the pool as stopping (`is_running()` becomes `false`).
2. Closes the queue. New `submit()` calls throw `PoolStoppedError`, and idle workers wake up.
3. Workers keep popping until the queue is **closed and empty**, so all queued tasks run.
4. Joins every worker. No thread is ever detached or leaked.

Other guarantees:

- If creating a worker thread fails in the constructor, the already-started workers are stopped and joined before the exception propagates. Otherwise `std::thread`'s destructor would call `std::terminate`.
- The pool is non-copyable and non-movable. Workers hold `this`, so moving the pool would leave them pointing at a dead object.
- Calling `shutdown()` from inside a task throws `std::logic_error`. Without that check, the call would wait on its own thread forever.

## Build

Requirements: CMake ≥ 3.21 (for presets), Ninja, and GCC ≥ 9 or Clang ≥ 10. GoogleTest v1.15.2 is downloaded automatically (pinned URL with SHA-256).

```sh
cmake --preset debug          # or: release, asan-ubsan, tsan
cmake --build --preset debug
```

Without presets:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

Choose the compiler with `-DCMAKE_CXX_COMPILER=clang++` (or `g++`).

## Tests

```sh
ctest --preset debug
# repeat to shake out intermittent concurrency bugs:
ctest --preset debug --repeat until-fail:10
# or run the binary directly, e.g. one suite:
./build/debug/tests/thread_pool_tests --gtest_filter='ThreadPoolStress.*'
```

What's covered:

| Area | Tests |
|---|---|
| Basic execution and results | `ExecutesTaskAndReturnsResult`, `ForwardsArguments`, `ManySubmissionsAllProduceCorrectResults` |
| Argument handling | `AcceptsMoveOnlyCallablesAndArguments`, `ArgumentsAreCopiedUnlessWrappedInRef` |
| Return types / void | `SupportsDifferentReturnTypes`, `VoidTasksRunAndSignalCompletion` |
| Exceptions | `ExceptionsPropagateThroughFuture`, `VoidTaskExceptionPropagates`, `NonStdExceptionPropagates`, `WorkersSurviveThrowingTasks` |
| Shutdown | `SubmitAfterShutdownThrows`, `ShutdownIsIdempotent`, `ShutdownDrainsPendingTasks`, `ConcurrentShutdownCallsAllWaitForTheJoin`, `ShutdownFromWorkerThrowsInsteadOfDeadlocking` |
| Destruction under load | `DestructorUnderLoadRunsEveryAcceptedTask`, `DestructorWhileProducersAreSubmitting` |
| Nested tasks (`wait`) | `OffPoolThreadBehavesLikeGet`, `NestedWaitOnSingleWorkerDoesNotDeadlock`, `AllWorkersWaitingOnSubtasksDoNotStarve`, `RecursiveDivideAndConquer`, `NestedExceptionPropagatesThroughWait`, `ShutdownWhileTasksAreHelping` |
| Stress / races | `MultipleProducersThousandsOfTasks` (20,000 tasks from 8 producers), `SharedCounterWithMutexHasNoLostUpdates`, `RepeatedCreateAndDestroy`, `TasksRunOnWorkerThreadsConcurrently` |
| Queue | FIFO, `try_pop`, move-only types, close/drain, `PopBlocksUntilPush`, `CloseWakesAllBlockedConsumers`, MPMC no-loss, per-producer order |

Deadlock checks use `future::wait_for` with timeouts, so a deadlock shows up as a test failure instead of a hung run. CTest also enforces a 120-second timeout per test.

## Sanitizers

```sh
# AddressSanitizer + UndefinedBehaviorSanitizer
cmake --preset asan-ubsan && cmake --build --preset asan-ubsan && ctest --preset asan-ubsan

# ThreadSanitizer (cannot be combined with ASan)
cmake --preset tsan && cmake --build --preset tsan && ctest --preset tsan
```

The test presets set `halt_on_error`, so any sanitizer report fails the test.

`tests/tsan.supp` suppresses one known TSan **false positive**. libstdc++ is not TSan-instrumented, so TSan can't see the atomic reference count of an exception that is passed through a future. It then sometimes reports the exception's destructor (on a worker) as racing with `e.what()` (on the caller). Only the destructors of `std::runtime_error` and `std::logic_error` are suppressed. This was observed once in CI with GCC 13 and couldn't be reproduced locally in 300 runs.

**TSan on recent Linux kernels:** if TSan aborts with `FATAL: ThreadSanitizer: unexpected memory mapping`, the kernel's ASLR entropy is too high for the TSan runtime. Use either of these fixes:

```sh
sudo sysctl vm.mmap_rnd_bits=28      # system-wide (what CI does)
setarch -R ctest --preset tsan       # this process only (in Docker add --security-opt seccomp=unconfined)
```

Sanitizers need Linux (or macOS) with GCC or Clang. For Clang on Ubuntu, install `libclang-rt-<version>-dev`.

## Example

```cpp
#include "thread_pool.hpp"
#include <iostream>
#include <numeric>
#include <vector>

int main() {
    tp::ThreadPool pool;  // hardware_concurrency() workers

    // Parallel sum of chunks.
    std::vector<int> data(1'000'000, 1);
    std::vector<std::future<long long>> parts;
    const std::size_t chunk = data.size() / 8;
    for (std::size_t i = 0; i < 8; ++i) {
        parts.push_back(pool.submit([&data, i, chunk] {
            return std::accumulate(data.begin() + i * chunk, data.begin() + (i + 1) * chunk, 0LL);
        }));
    }
    long long total = 0;
    for (auto& p : parts) total += p.get();
    std::cout << total << '\n';  // 1000000

    // Exceptions come back through the future.
    auto bad = pool.submit([] { throw std::runtime_error("failed"); });
    try { bad.get(); } catch (const std::exception& e) { std::cout << e.what() << '\n'; }

    // Share state by reference explicitly.
    int counter = 0;
    pool.submit([](int& c) { ++c; }, std::ref(counter)).get();

    pool.shutdown();  // optional, the destructor does the same
}
```

Link against the `thread_pool` target from your CMake project:

```cmake
add_subdirectory(thread-pool-cpp)
target_link_libraries(my_app PRIVATE thread_pool::thread_pool)
```

## Known limitations and trade-offs

- **Single global queue.** Every push and pop takes the same mutex, so throughput for very small tasks (well under a microsecond each) is limited by contention. Per-worker queues with work stealing would scale better, at the cost of much more complexity.
- **Unbounded queue, no back-pressure.** A producer can enqueue faster than workers drain and grow memory without limit. A bounded queue with a blocking or failing `push` would fix that.
- **No cancellation or priorities.** Once submitted, a task will run. FIFO order only.
- **Shutdown waits for every task.** A task that never returns makes `shutdown()` (and the destructor) block forever. There's no timeout or "discard pending" mode.
- **Use `pool.wait(f)`, not `f.get()`, inside tasks.** Plain `get()` on a same-pool future from a worker can still starve the pool. `wait()` prevents that for acyclic task dependencies. A helping worker may run an unrelated long task before noticing its own result is ready, which adds latency.
- **Destroying the pool from one of its own tasks** calls `std::terminate`, because the destructor can't throw. `shutdown()` from a task throws `std::logic_error`.
- **Fixed size.** The thread count can't change after construction.
- Benchmarks aren't included, so no performance claims are made.

## License

MIT. See [LICENSE](LICENSE).
