#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "thread_safe_queue.hpp"

namespace tp {

// Thrown by submit() once the pool has started shutting down.
class PoolStoppedError : public std::runtime_error {
public:
    PoolStoppedError() : std::runtime_error("thread pool is shut down; task rejected") {}
};

// Fixed-size pool of worker threads that execute tasks from a shared queue.
//
//   tp::ThreadPool pool(4);
//   std::future<int> f = pool.submit([](int a, int b) { return a + b; }, 2, 3);
//   int five = f.get();
//
// Shutdown (explicit or via the destructor) stops accepting tasks, lets the
// workers finish every task that was already accepted, then joins them.
class ThreadPool {
public:
    // Throws std::invalid_argument if thread_count == 0.
    explicit ThreadPool(std::size_t thread_count = default_thread_count());
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    // Schedules f(args...) and returns a future for its result. The callable
    // and arguments are decay-copied/moved into the task (like std::async), so
    // pass std::ref(x) to share an object by reference.
    //
    // Exceptions thrown by the task are stored in the future and rethrown by
    // future::get(); they never escape into the worker thread.
    //
    // Throws PoolStoppedError if shutdown() has already begun.
    template <typename F, typename... Args>
    auto submit(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>>;

    // Waits for `future` and returns its result (rethrowing its exception),
    // like future.get(). Use this instead of get() when waiting *inside* a
    // task on another task of the same pool: on a worker thread it keeps
    // running queued tasks while the result is not ready, so a task waiting
    // on its subtasks cannot starve the pool (with plain get(), N tasks
    // waiting on queued subtasks would block all N workers forever).
    // Off the pool's threads it is exactly future.get().
    template <typename R>
    R wait(std::future<R>& future);

    // Stops accepting tasks, drains the queue and joins all workers.
    // Idempotent and safe to call from several threads at once.
    // Must not be called from inside a task (it would wait for itself);
    // doing so throws std::logic_error instead of deadlocking.
    void shutdown();

    std::size_t thread_count() const noexcept { return thread_count_; }
    std::size_t pending_tasks() const { return tasks_.size(); }
    bool is_running() const noexcept { return !stopping_.load(std::memory_order_acquire); }

    static std::size_t default_thread_count() noexcept;

private:
    using Task = std::function<void()>;

    void worker_loop();
    // Runs one queued task on the calling thread. Returns false if none is queued.
    bool run_pending_task();
    bool is_worker_thread() const;

    const std::size_t thread_count_;
    ThreadSafeQueue<Task> tasks_;
    std::vector<std::thread> workers_;
    // Copy of the workers' ids, written only in the constructor. shutdown()
    // reads it instead of workers_ because join() mutates the std::thread
    // objects, which would race with a concurrent shutdown() call.
    std::vector<std::thread::id> worker_ids_;

    // Set once shutdown() begins; read lock-free by is_running().
    std::atomic<bool> stopping_{false};
    // Serialises shutdown() so concurrent callers all return only after the join.
    std::mutex shutdown_mutex_;
};

template <typename F, typename... Args>
auto ThreadPool::submit(F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>> {
    using Result = std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>;

    // Bind the callable and its arguments into a nullary function. Init-capture
    // with std::forward moves rvalues and copies lvalues exactly once.
    auto bound = [fn = std::forward<F>(f),
                  bound_args = std::make_tuple(std::forward<Args>(args)...)]() mutable -> Result {
        return std::apply(std::move(fn), std::move(bound_args));
    };

    // packaged_task is move-only but std::function requires copyable targets,
    // so the task lives behind a shared_ptr. packaged_task::operator() catches
    // any exception and stores it in the shared state for future::get().
    auto task = std::make_shared<std::packaged_task<Result()>>(std::move(bound));
    std::future<Result> result = task->get_future();

    // push() fails atomically with close(), so there is no window in which a
    // task is accepted after shutdown started and then silently dropped.
    if (!tasks_.push([task] { (*task)(); })) {
        throw PoolStoppedError();
    }
    return result;
}

template <typename R>
R ThreadPool::wait(std::future<R>& future) {
    if (is_worker_thread()) {
        // Help until the result is ready. When the queue is empty, the task we
        // depend on has already been taken by some worker, which is itself
        // either running it or helping in the same way, so blocking is safe.
        while (future.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
            if (!run_pending_task()) {
                future.wait();
                break;
            }
        }
    }
    return future.get();
}

}  // namespace tp
