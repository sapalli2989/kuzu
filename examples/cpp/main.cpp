#include <iostream>

#include <random>

#include "common/timer.h"
#include "storage/index/hash_index_builder.h"

using namespace kuzu::common;
using namespace kuzu::storage;

int main() {
    auto pkIndex = std::make_unique<PrimaryKeyIndexBuilder>("./test.hindex", *LogicalType::INT64());

    int64_t num = 100000000;
    std::vector<int64_t> keys;
    keys.reserve(num);
    std::mt19937_64 rng(0);
    std::uniform_int_distribution<int64_t> dist(0, num * 10 - 1);
    for (int64_t i = 0; i < num; i++) {
        keys.push_back(dist(rng));
    }

    pkIndex->bulkReserve(num);
    Timer timer;
    timer.start();
    for (int64_t i = 0; i < num; i++) {
        pkIndex->append(keys[i], i);
    }
    auto timeSpent = timer.getElapsedTimeInMS();
    std::cout << "Time spent: " << timeSpent << " ms" << std::endl;

    pkIndex->flush();
}
