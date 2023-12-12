#include <iostream>
#include <random>
#include <thread>

#include "common/timer.h"
#include "robinhood.h"
#include "storage/index/hash_index_builder.h"
#include <latch>

using namespace kuzu::common;
using namespace kuzu::storage;

struct Func {
    static void buildRobinHood(std::latch* latch, const std::vector<int64_t>& keys,
        robin_hood::unordered_map<int64_t, int64_t> map, int64_t start, int64_t numValues) {
        latch->arrive_and_wait();

        for (int64_t i = 0; i < numValues; i++) {
            map.insert({keys[i + start], i});
        }
    }

    static void buildHashIndex(std::latch* latch, const std::vector<int64_t>& keys,
        std::unique_ptr<PrimaryKeyIndexBuilder> indexBuilder, int64_t start, int64_t numValues) {
        latch->arrive_and_wait();

        for (int64_t i = 0; i < numValues; i++) {
            indexBuilder->append(keys[i + start], i);
        }
    }
};

void testRobinHood(const std::vector<int64_t>& keys, int64_t num, int64_t numParts) {
    std::vector<std::unique_ptr<std::thread>> threads;
    threads.reserve(numParts);

    std::latch latch(numParts);

    for (auto i = 0u; i < numParts; i++) {
        auto length = num / numParts;
        auto start = i * length;
        auto mp = robin_hood::unordered_map<int64_t, int64_t>();
        mp.reserve(length);
        threads.push_back(std::make_unique<std::thread>(
            Func::buildRobinHood, &latch, keys, std::move(mp), start, length));
    }
    Timer timer;
    timer.start();
    for (auto& thread : threads) {
        thread->join();
    }
    auto timeSpent = timer.getElapsedTimeInMS();
    std::cout << "End to end time spent: " << timeSpent << " ms" << std::endl;
}

void testHashIndex(const std::vector<int64_t>& keys, int64_t num, int64_t numParts) {
    std::vector<std::unique_ptr<std::thread>> threads;
    threads.reserve(numParts);

    std::latch latch(numParts);

    for (auto i = 0u; i < numParts; i++) {
        auto length = num / numParts;
        auto start = i * length;
        auto hindex = std::make_unique<PrimaryKeyIndexBuilder>(
            "./test" + std::to_string(i) + ".hindex", *LogicalType::INT64());
        hindex->bulkReserve(length);
        threads.push_back(std::make_unique<std::thread>(
            Func::buildHashIndex, &latch, keys, std::move(hindex), start, length));
    }
    Timer timer;
    timer.start();
    for (auto& thread : threads) {
        thread->join();
    }
    auto timeSpent = timer.getElapsedTimeInMS();
    std::cout << "End to end time spent: " << timeSpent << " ms" << std::endl;
}

int main(int /*argc*/, char** argv) {
    int64_t numParts = atoi(argv[1]);
    int64_t num = 100'000'000;
    std::vector<int64_t> keys;
    keys.reserve(num);
    std::mt19937_64 rng(0);
    std::uniform_int_distribution<int64_t> dist(0, num * 10 - 1);
    for (int64_t i = 0; i < num; i++) {
        keys.push_back(dist(rng));
    }

    // testRobinHood(keys, num, numParts);
    testHashIndex(keys, num, numParts);
}
