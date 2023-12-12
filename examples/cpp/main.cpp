#include <iostream>
#include <random>

#include "common/timer.h"
#include "robinhood.h"
#include "storage/index/hash_index_builder.h"

using namespace kuzu::common;
using namespace kuzu::storage;

void testRobinHood(const std::vector<int64_t>& keys) {
    auto robinHoodMap = std::make_unique<robin_hood::unordered_map<int64_t, int64_t>>();
    Timer timer;
    timer.start();
    for (int64_t i = 0; i < keys.size(); i++) {
        robinHoodMap->insert({keys[i], i});
    }
    auto timeSpent = timer.getElapsedTimeInMS();
    std::cout << "Time spent: " << timeSpent << " ms" << std::endl;
}

void testHashIndex(const std::vector<int64_t>& keys) {
    auto pkIndex = std::make_unique<PrimaryKeyIndexBuilder>("./test.hindex", *LogicalType::INT64());
    pkIndex->bulkReserve(keys.size());
    Timer timer;
    timer.start();
    for (int64_t i = 0; i < keys.size(); i++) {
        pkIndex->append(keys[i], i);
    }
    auto timeSpent = timer.getElapsedTimeInMS();
    std::cout << "Time spent: " << timeSpent << " ms" << std::endl;
    pkIndex->flush();
}

int main() {
    int64_t num = 100000000;
    std::vector<int64_t> keys;
    keys.reserve(num);
    std::mt19937_64 rng(0);
    std::uniform_int_distribution<int64_t> dist(0, num * 10 - 1);
    for (int64_t i = 0; i < num; i++) {
        keys.push_back(dist(rng));
    }
    testRobinHood(keys);
//    testHashIndex(keys);
}
