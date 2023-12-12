#include <iostream>
#include <random>
#include <thread>

#include "common/timer.h"
#include "robinhood.h"
#include "storage/index/hash_index_builder.h"

using namespace kuzu::common;
using namespace kuzu::storage;

struct Func {
    static void buildRobinHood(const std::vector<int64_t>& keys,
        robin_hood::unordered_map<int64_t, int64_t>* map, int64_t start, int64_t numValues) {
        Timer timer;
        timer.start();
        for (int64_t i = 0; i < numValues; i++) {
            map->insert({keys[i + start], i});
        }
        auto timeSpent = timer.getElapsedTimeInMS();
        std::cout << "Time spent: " << timeSpent << " ms"
                  << ", num values: " << numValues << std::endl;
    }

    static void buildHashIndex(const std::vector<int64_t>& keys,
        PrimaryKeyIndexBuilder* indexBuilder, int64_t start, int64_t numValues) {
        Timer timer;
        timer.start();
        for (int64_t i = 0; i < numValues; i++) {
            indexBuilder->append(keys[i + start], i);
        }
        auto timeSpent = timer.getElapsedTimeInMS();
        std::cout << "Time spent: " << timeSpent << " ms"
                  << ", num values: " << numValues << std::endl;
    }
};

void testRobinHood(const std::vector<int64_t>& keys, int64_t num, int64_t numParts) {
    std::vector<std::unique_ptr<robin_hood::unordered_map<int64_t, int64_t>>> robinHoods;
    robinHoods.reserve(numParts);
    for (auto i = 0u; i < numParts; i++) {
        robinHoods.push_back(std::make_unique<robin_hood::unordered_map<int64_t, int64_t>>());
    }

    std::vector<std::unique_ptr<std::thread>> threads;
    threads.reserve(numParts);

    Timer timer;
    timer.start();
    for (auto i = 0u; i < numParts; i++) {
        auto length = num / numParts;
        auto start = i * length;
        threads.push_back(std::make_unique<std::thread>(
            Func::buildRobinHood, keys, robinHoods[i].get(), start, length));
    }
    for (auto& thread : threads) {
        thread->join();
    }
    auto timeSpent = timer.getElapsedTimeInMS();
    std::cout << "End to end time spent: " << timeSpent << " ms" << std::endl;
}

void testHashIndex(const std::vector<int64_t>& keys, int64_t num, int64_t numParts) {
    std::vector<std::unique_ptr<PrimaryKeyIndexBuilder>> hashIndexes;
    hashIndexes.reserve(numParts);
    for (auto i = 0u; i < numParts; i++) {
        hashIndexes.push_back(std::make_unique<PrimaryKeyIndexBuilder>(
            "./test" + std::to_string(i) + ".hindex", *LogicalType::INT64()));
    }

    std::vector<std::unique_ptr<std::thread>> threads;
    threads.reserve(numParts);

    Timer timer;
    timer.start();
    for (auto i = 0u; i < numParts; i++) {
        auto length = num / numParts;
        auto start = i * length;
        hashIndexes[i]->bulkReserve(length);
        threads.push_back(std::make_unique<std::thread>(
            Func::buildHashIndex, keys, hashIndexes[i].get(), start, length));
    }
    for (auto& thread : threads) {
        thread->join();
    }
    auto timeSpent = timer.getElapsedTimeInMS();
    std::cout << "End to end time spent: " << timeSpent << " ms" << std::endl;
}

int main() {
    int64_t numParts = 6;
    int64_t num = 100000000;
    std::vector<int64_t> keys;
    keys.reserve(num);
    std::mt19937_64 rng(0);
    std::uniform_int_distribution<int64_t> dist(0, num * 10 - 1);
    for (int64_t i = 0; i < num; i++) {
        keys.push_back(dist(rng));
    }

    testRobinHood(keys, num, numParts);
    testHashIndex(keys, num, numParts);
}
