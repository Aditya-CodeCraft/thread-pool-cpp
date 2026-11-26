#include "thread_pool.hpp"

#include <algorithm>

namespace tp {

std::size_t ThreadPool::default_thread_count() noexcept {
    // hardware_concurrency() may return 0 when the value is not computable.
    return std::max<std::size_t>(1, std::thread::hardware_concurrency());
}

ThreadPool::ThreadPool(std::size_t thread_count) : thread_count_(thread_count) {
    if (thread_count == 0) {
        throw std::invalid_argument("ThreadPool needs at least one worker thread");
    }
    workers_.reserve(thread_count);
    worker_ids_.reserve(thread_count);
    try {
        for (std::size_t i = 0; i < thread_count; ++i) {
            workers_.emplace_back(&ThreadPool::worker_loop, this);
            worker_ids_.push_back(workers_.back().get_id());
        }
    } catch (...) {
        // Thread creation failed part-way: the destructor will not run for a
        // partially constructed object, so stop and join the started workers
        // here, otherwise std::thread's destructor would call std::terminate.
        tasks_.close();
        for (auto& worker : workers_) {
            worker.join();
        }
        throw;
    }
}

ThreadPool::~ThreadPool() {
    // A destructor must not throw. shutdown() only throws when called from a
    // worker thread, which would mean a task destroyed its own pool; there is
    // nothing safe to do in that case except let terminate() report it.
    shutdown();
}

void ThreadPool::shutdown() {
    if (is_worker_thread()) {
        throw std::logic_error("ThreadPool::shutdown() called from one of its own worker threads");
    }
    std::lock_guard<std::mutex> lock(shutdown_mutex_);
    stopping_.store(true, std::memory_order_release);
    tasks_.close();  // rejects new submits, wakes idle workers
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();  // returns after the worker drained the queue
        }
    }
}

void ThreadPool::worker_loop() {
    // pop() returns nullopt only when the queue is closed *and* empty, so every
    // accepted task runs before the worker exits.
    while (std::optional<Task> task = tasks_.pop()) {
        // Cannot throw: the wrapped packaged_task stores exceptions in its future.
        (*task)();
    }
}

bool ThreadPool::is_worker_thread() const {
    const auto self = std::this_thread::get_id();
    return std::find(worker_ids_.begin(), worker_ids_.end(), self) != worker_ids_.end();
}

}  // namespace tp
