#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>
#include <utility>

namespace tp {

// Unbounded multi-producer / multi-consumer blocking FIFO queue.
//
// Lifecycle: open -> closed. After close():
//   * push() is rejected (returns false) so no new work can sneak in;
//   * pop() keeps returning the items that are already queued and returns
//     std::nullopt only once the queue is closed AND empty. That "drain then
//     stop" rule is what lets a consumer loop `while (auto x = q.pop())` finish
//     all accepted work and then exit without any extra flag.
template <typename T>
class ThreadSafeQueue {
public:
    ThreadSafeQueue() = default;
    ThreadSafeQueue(const ThreadSafeQueue&) = delete;
    ThreadSafeQueue& operator=(const ThreadSafeQueue&) = delete;

    // Returns false (and drops nothing: `value` is not moved from) if closed.
    bool push(T&& value) { return emplace(std::move(value)); }
    bool push(const T& value) { return emplace(value); }

    template <typename... Args>
    bool emplace(Args&&... args) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) {
                return false;
            }
            items_.emplace_back(std::forward<Args>(args)...);
        }
        // Notify after unlocking so the woken consumer does not immediately
        // block on the mutex we still hold.
        not_empty_.notify_one();
        return true;
    }

    // Blocks until an item is available or the queue is closed and drained.
    std::optional<T> pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        // The predicate form re-checks the condition, so spurious wake-ups are harmless.
        not_empty_.wait(lock, [this] { return closed_ || !items_.empty(); });
        return take_front_locked();
    }

    // Non-blocking: returns std::nullopt if nothing is queued right now.
    std::optional<T> try_pop() {
        std::lock_guard<std::mutex> lock(mutex_);
        return take_front_locked();
    }

    // Idempotent. Wakes every blocked consumer so they can drain and exit.
    void close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        not_empty_.notify_all();
    }

    bool closed() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return closed_;
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return items_.size();
    }

    bool empty() const { return size() == 0; }

private:
    std::optional<T> take_front_locked() {
        if (items_.empty()) {
            return std::nullopt;
        }
        std::optional<T> front(std::move(items_.front()));
        items_.pop_front();
        return front;
    }

    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::deque<T> items_;
    bool closed_ = false;  // guarded by mutex_
};

}  // namespace tp
