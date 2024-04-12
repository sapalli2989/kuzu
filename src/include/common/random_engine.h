#pragma once

#include "pcg_random.hpp"
#include "common/mutex.h"

namespace kuzu {

namespace main {
class ClientContext;
}

namespace common {

struct RandomState {
    pcg32 pcg;

    RandomState() {}
};

class RandomEngine {
public:
    RandomEngine();
    uint32_t nextRandomInteger();
private:
    RandomState randomState;
    std::mutex lock;
};
} // namespace common
} // namespace kuzu
