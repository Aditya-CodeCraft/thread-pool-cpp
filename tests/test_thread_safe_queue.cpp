#include "thread_safe_queue.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using tp::ThreadSafeQueue;

TEST(ThreadSafeQueue, PushPopIsFifo) {
    ThreadSafeQueue<int> q;
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(q.push(i));
    }
    EXPECT_EQ(q.size(), 5u);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(q.pop(), i);
    }
    EXPECT_TRUE(q.empty());
}

TEST(ThreadSafeQueue, TryPopOnEmptyReturnsNullopt) {
    ThreadSafeQueue<int> q;
    EXPECT_EQ(q.try_pop(), std::nullopt);
    q.push(7);
    EXPECT_EQ(q.try_pop(), 7);
}

TEST(ThreadSafeQueue, SupportsMoveOnlyTypes) {
    ThreadSafeQueue<std::unique_ptr<std::string>> q;
    ASSERT_TRUE(q.push(std::make_unique<std::string>("hello")));
    ASSERT_TRUE(q.emplace(new std::string("world")));
    EXPECT_EQ(**q.pop(), "hello");
    EXPECT_EQ(**q.pop(), "world");
}

TEST(ThreadSafeQueue, PushAfterCloseIsRejected) {
    ThreadSafeQueue<int> q;
    q.close();
    EXPECT_TRUE(q.closed());
    EXPECT_FALSE(q.push(1));
    EXPECT_TRUE(q.empty());
}

TEST(ThreadSafeQueue, CloseDrainsRemainingItemsBeforeReturningNullopt) {
    ThreadSafeQueue<int> q;
    q.push(1);
    q.push(2);
    q.close();
    EXPECT_EQ(q.pop(), 1);
    EXPECT_EQ(q.pop(), 2);
    EXPECT_EQ(q.pop(), std::nullopt);  // closed and empty: does not block
}

TEST(ThreadSafeQueue, CloseIsIdempotent) {
    ThreadSafeQueue<int> q;
    q.close();
    q.close();
    EXPECT_EQ(q.pop(), std::nullopt);
}

TEST(ThreadSafeQueue, PopBlocksUntilPush) {
    ThreadSafeQueue<int> q;
    auto consumer = std::async(std::launch::async, [&q] { return q.pop(); });

    // The consumer has nothing to take, so it must still be waiting.
    EXPECT_EQ(consumer.wait_for(100ms), std::future_status::timeout);

    q.push(42);
    ASSERT_EQ(consumer.wait_for(5s), std::future_status::ready) << "push did not wake the consumer";
    EXPECT_EQ(consumer.get(), 42);
}

TEST(ThreadSafeQueue, CloseWakesAllBlockedConsumers) {
    ThreadSafeQueue<int> q;
    std::vector<std::future<std::optional<int>>> consumers;
    for (int i = 0; i < 4; ++i) {
        consumers.push_back(std::async(std::launch::async, [&q] { return q.pop(); }));
    }
    for (auto& c : consumers) {
        EXPECT_EQ(c.wait_for(50ms), std::future_status::timeout);
    }

    q.close();
    for (auto& c : consumers) {
        ASSERT_EQ(c.wait_for(5s), std::future_status::ready) << "close did not wake a consumer";
        EXPECT_EQ(c.get(), std::nullopt);
    }
}

TEST(ThreadSafeQueue, MultipleProducersMultipleConsumersLoseNothing) {
    constexpr int kProducers = 4;
    constexpr int kConsumers = 4;
    constexpr int kPerProducer = 10000;
    ThreadSafeQueue<int> q;

    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&q, p] {
            for (int i = 0; i < kPerProducer; ++i) {
                q.push(p * kPerProducer + i);
            }
        });
    }

    std::vector<std::vector<int>> received(kConsumers);
    std::vector<std::thread> consumers;
    for (int c = 0; c < kConsumers; ++c) {
        consumers.emplace_back([&q, &received, c] {
            while (auto v = q.pop()) {
                received[c].push_back(*v);
            }
        });
    }

    for (auto& t : producers) t.join();
    q.close();  // consumers drain the rest, then exit
    for (auto& t : consumers) t.join();

    std::set<int> all;
    std::size_t total = 0;
    for (const auto& r : received) {
        total += r.size();
        all.insert(r.begin(), r.end());
    }
    EXPECT_EQ(total, static_cast<std::size_t>(kProducers * kPerProducer)) << "item lost or duplicated";
    EXPECT_EQ(all.size(), static_cast<std::size_t>(kProducers * kPerProducer)) << "duplicate item";
}

TEST(ThreadSafeQueue, PerProducerOrderIsPreserved) {
    // FIFO across a single consumer: items from one producer keep their order.
    constexpr int kPerProducer = 5000;
    ThreadSafeQueue<std::pair<int, int>> q;
    std::thread a([&q] { for (int i = 0; i < kPerProducer; ++i) q.push({0, i}); });
    std::thread b([&q] { for (int i = 0; i < kPerProducer; ++i) q.push({1, i}); });
    a.join();
    b.join();
    q.close();

    int last[2] = {-1, -1};
    while (auto item = q.pop()) {
        EXPECT_GT(item->second, last[item->first]);
        last[item->first] = item->second;
    }
    EXPECT_EQ(last[0], kPerProducer - 1);
    EXPECT_EQ(last[1], kPerProducer - 1);
}
