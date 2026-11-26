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
