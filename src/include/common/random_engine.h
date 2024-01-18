#pragma once
#include <memory>
#include <mutex>

#include "pcg_random.hpp"

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
    std::unique_ptr<RandomState> randomState;
};
} // namespace common
} // namespace kuzu
