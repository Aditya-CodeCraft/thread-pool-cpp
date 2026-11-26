#include "thread_pool.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using tp::ThreadPool;

namespace {

// A one-shot gate: tasks wait on it so tests control exactly when they finish.
class Gate {
public:
    void open() { promise_.set_value(); }
    void wait() const { future_.wait(); }

private:
    std::promise<void> promise_;
    std::shared_future<void> future_ = promise_.get_future().share();
};

int add(int a, int b) { return a + b; }

}  // namespace

// ---- construction -----------------------------------------------------------

TEST(ThreadPool, ZeroThreadsIsRejected) {
    EXPECT_THROW(ThreadPool(0), std::invalid_argument);
}

TEST(ThreadPool, ReportsThreadCount) {
    ThreadPool pool(3);
    EXPECT_EQ(pool.thread_count(), 3u);
    EXPECT_TRUE(pool.is_running());
    EXPECT_GE(ThreadPool::default_thread_count(), 1u);
}

// ---- basic execution and results ------------------------------------------

TEST(ThreadPool, ExecutesTaskAndReturnsResult) {
    ThreadPool pool(2);
    auto f = pool.submit([] { return 42; });
    EXPECT_EQ(f.get(), 42);
}

TEST(ThreadPool, ForwardsArguments) {
    ThreadPool pool(2);
    EXPECT_EQ(pool.submit(add, 2, 3).get(), 5);
    EXPECT_EQ(pool.submit([](const std::string& s, int n) { return s + std::to_string(n); },
                          std::string("n="), 7)
                  .get(),
              "n=7");
}

TEST(ThreadPool, AcceptsMoveOnlyCallablesAndArguments) {
    ThreadPool pool(2);
    auto owned = std::make_unique<int>(10);
    auto f = pool.submit([p = std::move(owned)](std::unique_ptr<int> q) { return *p + *q; },
                         std::make_unique<int>(5));
    EXPECT_EQ(f.get(), 15);
}

TEST(ThreadPool, ArgumentsAreCopiedUnlessWrappedInRef) {
    ThreadPool pool(1);
    int value = 1;
    // Like std::async, lvalue arguments are copied into the task; a callable
    // taking `int&` would not even compile without std::ref.
    EXPECT_EQ(pool.submit([](int v) { return v += 99; }, value).get(), 100);
    EXPECT_EQ(value, 1);
    pool.submit([](int& v) { v = 100; }, std::ref(value)).get();
    EXPECT_EQ(value, 100);
}

TEST(ThreadPool, SupportsDifferentReturnTypes) {
    ThreadPool pool(4);
    std::future<int> i = pool.submit([] { return 1; });
    std::future<double> d = pool.submit([] { return 2.5; });
    std::future<std::string> s = pool.submit([] { return std::string("three"); });
    std::future<std::vector<int>> v = pool.submit([] { return std::vector<int>{4, 4}; });
    std::future<std::unique_ptr<int>> u = pool.submit([] { return std::make_unique<int>(5); });

    EXPECT_EQ(i.get(), 1);
    EXPECT_DOUBLE_EQ(d.get(), 2.5);
    EXPECT_EQ(s.get(), "three");
    EXPECT_EQ(v.get(), (std::vector<int>{4, 4}));
    EXPECT_EQ(*u.get(), 5);
}

TEST(ThreadPool, VoidTasksRunAndSignalCompletion) {
    ThreadPool pool(2);
    std::atomic<int> counter{0};
    std::future<void> f = pool.submit([&counter] { counter.fetch_add(1); });
    static_assert(std::is_same_v<decltype(f), std::future<void>>);
    f.get();
    EXPECT_EQ(counter.load(), 1);
}

TEST(ThreadPool, ManySubmissionsAllProduceCorrectResults) {
    ThreadPool pool(4);
    std::vector<std::future<int>> futures;
    for (int i = 0; i < 1000; ++i) {
        futures.push_back(pool.submit([i] { return i * i; }));
    }
    for (int i = 0; i < 1000; ++i) {
        EXPECT_EQ(futures[i].get(), i * i);
    }
}

TEST(ThreadPool, TasksRunOnWorkerThreadsConcurrently) {
    // Two tasks that each wait for the other can only finish if they run at the
    // same time on different workers.
    ThreadPool pool(2);
    std::promise<void> a_started, b_started;
    auto a = pool.submit([&] { a_started.set_value(); b_started.get_future().wait(); });
    auto b = pool.submit([&] { b_started.set_value(); a_started.get_future().wait(); });
    ASSERT_EQ(a.wait_for(10s), std::future_status::ready);
    ASSERT_EQ(b.wait_for(10s), std::future_status::ready);
}

// ---- exceptions --------------------------------------------------------------

TEST(ThreadPool, ExceptionsPropagateThroughFuture) {
    ThreadPool pool(2);
    auto f = pool.submit([]() -> int { throw std::runtime_error("boom"); });
    try {
        f.get();
        FAIL() << "expected an exception";
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(e.what(), "boom");
    }
}

TEST(ThreadPool, VoidTaskExceptionPropagates) {
    ThreadPool pool(1);
    auto f = pool.submit([] { throw std::logic_error("void boom"); });
    EXPECT_THROW(f.get(), std::logic_error);
}

TEST(ThreadPool, NonStdExceptionPropagates) {
    ThreadPool pool(1);
    auto f = pool.submit([] { throw 7; });
    EXPECT_THROW(f.get(), int);
}

TEST(ThreadPool, WorkersSurviveThrowingTasks) {
    ThreadPool pool(2);
    std::vector<std::future<void>> failures;
    for (int i = 0; i < 100; ++i) {
        failures.push_back(pool.submit([] { throw std::runtime_error("x"); }));
    }
    for (auto& f : failures) {
        EXPECT_THROW(f.get(), std::runtime_error);
    }
    // The pool must still work afterwards with all of its threads.
    EXPECT_EQ(pool.submit([] { return 1; }).get(), 1);
}

// ---- shutdown ------------------------------------------------------------

TEST(ThreadPool, SubmitAfterShutdownThrows) {
    ThreadPool pool(2);
    pool.shutdown();
    EXPECT_FALSE(pool.is_running());
    EXPECT_THROW(pool.submit([] { return 1; }), tp::PoolStoppedError);
}

TEST(ThreadPool, ShutdownIsIdempotent) {
    ThreadPool pool(2);
    pool.shutdown();
    pool.shutdown();  // second call is a no-op; destructor calls it a third time
}

TEST(ThreadPool, ShutdownDrainsPendingTasks) {
    ThreadPool pool(1);
    Gate gate;
    std::atomic<int> done{0};

    // The single worker blocks on the gate, so the next 50 tasks stay queued.
    auto blocker = pool.submit([&gate] { gate.wait(); });
    std::vector<std::future<void>> queued;
    for (int i = 0; i < 50; ++i) {
        queued.push_back(pool.submit([&done] { done.fetch_add(1); }));
    }
    EXPECT_GT(pool.pending_tasks(), 0u);

    auto shutting_down = std::async(std::launch::async, [&pool] { pool.shutdown(); });
    // shutdown() must wait for the queued work rather than drop it.
    EXPECT_EQ(shutting_down.wait_for(100ms), std::future_status::timeout);
    EXPECT_THROW(pool.submit([] {}), tp::PoolStoppedError);  // already closed for new work

    gate.open();
    ASSERT_EQ(shutting_down.wait_for(10s), std::future_status::ready) << "shutdown deadlocked";
    EXPECT_EQ(done.load(), 50);
    for (auto& f : queued) {
        EXPECT_EQ(f.wait_for(0s), std::future_status::ready);  // no broken promises
    }
}

TEST(ThreadPool, ConcurrentShutdownCallsAllWaitForTheJoin) {
    ThreadPool pool(2);
    std::atomic<int> done{0};
    for (int i = 0; i < 200; ++i) {
        pool.submit([&done] { std::this_thread::sleep_for(100us); done.fetch_add(1); });
    }
    std::vector<std::thread> stoppers;
    std::atomic<int> observed_incomplete{0};
    for (int i = 0; i < 4; ++i) {
        stoppers.emplace_back([&] {
            pool.shutdown();
            if (done.load() != 200) observed_incomplete.fetch_add(1);
        });
    }
    for (auto& t : stoppers) t.join();
    EXPECT_EQ(observed_incomplete.load(), 0) << "a shutdown() call returned before the drain finished";
}

TEST(ThreadPool, ShutdownFromWorkerThrowsInsteadOfDeadlocking) {
    ThreadPool pool(2);
    auto f = pool.submit([&pool] { pool.shutdown(); });
    ASSERT_EQ(f.wait_for(10s), std::future_status::ready);
    EXPECT_THROW(f.get(), std::logic_error);
}

TEST(ThreadPool, DestructorUnderLoadRunsEveryAcceptedTask) {
    std::atomic<int> done{0};
    constexpr int kTasks = 5000;
    {
        ThreadPool pool(4);
        for (int i = 0; i < kTasks; ++i) {
            pool.submit([&done] { done.fetch_add(1, std::memory_order_relaxed); });
        }
        // pool goes out of scope while most tasks are still queued
    }
    EXPECT_EQ(done.load(), kTasks);
}

TEST(ThreadPool, DestructorWhileProducersAreSubmitting) {
    // Producers race with destruction: every submit must either be accepted (and
    // then run) or rejected with PoolStoppedError. Nothing may be lost.
    std::atomic<int> accepted{0};
    std::atomic<int> rejected{0};
    std::atomic<int> executed{0};
    std::atomic<bool> stop_producers{false};
    std::vector<std::thread> producers;
    {
        ThreadPool pool(2);
        for (int p = 0; p < 4; ++p) {
            producers.emplace_back([&] {
                while (!stop_producers.load()) {
                    try {
                        pool.submit([&executed] { executed.fetch_add(1); });
                        accepted.fetch_add(1);
                    } catch (const tp::PoolStoppedError&) {
                        rejected.fetch_add(1);
                        return;
                    }
                }
            });
        }
        std::this_thread::sleep_for(20ms);
        pool.shutdown();  // producers now get rejected and exit
        for (auto& t : producers) t.join();
    }
    EXPECT_GT(accepted.load(), 0);
    EXPECT_EQ(rejected.load(), 4);
    EXPECT_EQ(executed.load(), accepted.load());
}

// ---- nested tasks: wait() ------------------------------------------------

TEST(ThreadPoolWait, OffPoolThreadBehavesLikeGet) {
    ThreadPool pool(2);
    auto f = pool.submit([] { return 7; });
    EXPECT_EQ(pool.wait(f), 7);
    auto v = pool.submit([] {});
    pool.wait(v);
    auto bad = pool.submit([]() -> int { throw std::runtime_error("x"); });
    EXPECT_THROW(pool.wait(bad), std::runtime_error);
}

TEST(ThreadPoolWait, NestedWaitOnSingleWorkerDoesNotDeadlock) {
    // With one worker, `inner.get()` here would deadlock: the only worker is
    // busy running `outer`, so `inner` would never be dequeued.
    ThreadPool pool(1);
    auto outer = pool.submit([&pool] {
        auto inner = pool.submit([] { return 20; });
        return pool.wait(inner) + 1;
    });
    ASSERT_EQ(outer.wait_for(10s), std::future_status::ready) << "nested wait deadlocked";
    EXPECT_EQ(outer.get(), 21);
}

TEST(ThreadPoolWait, AllWorkersWaitingOnSubtasksDoNotStarve) {
    // Every worker runs a parent that waits on children queued behind the other
    // parents: the classic fixed-pool starvation deadlock with plain get().
    constexpr int kParents = 16;
    constexpr int kChildren = 8;
    ThreadPool pool(2);
    std::vector<std::future<int>> parents;
    for (int p = 0; p < kParents; ++p) {
        parents.push_back(pool.submit([&pool] {
            std::vector<std::future<int>> children;
            for (int c = 0; c < kChildren; ++c) {
                children.push_back(pool.submit([c] { return c; }));
            }
            int sum = 0;
            for (auto& child : children) sum += pool.wait(child);
            return sum;
        }));
    }
    for (auto& f : parents) {
        ASSERT_EQ(f.wait_for(30s), std::future_status::ready) << "worker starvation deadlock";
        EXPECT_EQ(f.get(), kChildren * (kChildren - 1) / 2);
    }
}

namespace {
long long fib(ThreadPool& pool, int n) {
    if (n < 2) return n;
    auto left = pool.submit(fib, std::ref(pool), n - 1);
    long long right = fib(pool, n - 2);
    return pool.wait(left) + right;
}
}  // namespace

TEST(ThreadPoolWait, RecursiveDivideAndConquer) {
    ThreadPool pool(4);
    auto f = pool.submit(fib, std::ref(pool), 18);
    ASSERT_EQ(f.wait_for(60s), std::future_status::ready) << "recursive wait deadlocked";
    EXPECT_EQ(f.get(), 2584);
}

TEST(ThreadPoolWait, NestedExceptionPropagatesThroughWait) {
    ThreadPool pool(1);
    auto outer = pool.submit([&pool] {
        auto inner = pool.submit([]() -> int { throw std::runtime_error("inner"); });
        return pool.wait(inner);
    });
    ASSERT_EQ(outer.wait_for(10s), std::future_status::ready);
    EXPECT_THROW(outer.get(), std::runtime_error);
}

TEST(ThreadPoolWait, ShutdownWhileTasksAreHelping) {
    // The destructor closes the queue while parents are spawning children and
    // helping. Children are either accepted (and then run) or rejected; every
    // parent must still finish, and the destructor must not hang.
    std::atomic<int> parents_done{0};
    std::atomic<int> accepted{0};
    std::atomic<int> executed{0};
    {
        ThreadPool pool(3);
        for (int p = 0; p < 20; ++p) {
            pool.submit([&] {
                std::vector<std::future<void>> kids;
                for (int c = 0; c < 10; ++c) {
                    try {
                        kids.push_back(pool.submit([&executed] { executed.fetch_add(1); }));
                        accepted.fetch_add(1);
                    } catch (const tp::PoolStoppedError&) {
                        break;  // shutdown began: stop spawning, finish what we have
                    }
                }
                for (auto& k : kids) pool.wait(k);
                parents_done.fetch_add(1);
            });
        }
    }
    EXPECT_EQ(parents_done.load(), 20);
    EXPECT_EQ(executed.load(), accepted.load());
}

// ---- stress -------------------------------------------------------------

TEST(ThreadPoolStress, MultipleProducersThousandsOfTasks) {
    constexpr int kProducers = 8;
    constexpr int kPerProducer = 2500;
    ThreadPool pool(4);

    std::vector<std::future<long long>> results[kProducers];
    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&pool, &results, p] {
            for (int i = 0; i < kPerProducer; ++i) {
                long long n = static_cast<long long>(p) * kPerProducer + i;
                results[p].push_back(pool.submit([n] { return n * 2; }));
            }
        });
    }
    for (auto& t : producers) t.join();

    long long sum = 0;
    for (auto& per_producer : results) {
        for (auto& f : per_producer) {
            sum += f.get();
        }
    }
    const long long n = static_cast<long long>(kProducers) * kPerProducer;
    EXPECT_EQ(sum, n * (n - 1));  // 2 * (0 + 1 + ... + n-1)
}

TEST(ThreadPoolStress, SharedCounterWithMutexHasNoLostUpdates) {
    ThreadPool pool(8);
    std::mutex m;
    long long counter = 0;
    std::vector<std::future<void>> fs;
    for (int i = 0; i < 10000; ++i) {
        fs.push_back(pool.submit([&] {
            std::lock_guard<std::mutex> lock(m);
            ++counter;
        }));
    }
    for (auto& f : fs) f.get();
    EXPECT_EQ(counter, 10000);
}

TEST(ThreadPoolStress, RepeatedCreateAndDestroy) {
    // Catches races between worker start-up and immediate shutdown.
    for (int round = 0; round < 200; ++round) {
        ThreadPool pool(4);
        if (round % 2 == 0) {
            pool.submit([] {});
        }
    }
}
