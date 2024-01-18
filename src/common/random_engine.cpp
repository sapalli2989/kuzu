#include "common/random_engine.h"
#include "pcg_random.hpp"
#include <random>

namespace kuzu {
namespace common {

RandomEngine::RandomEngine() : randomState(std::unique_ptr<RandomState>()) {
    randomState->pcg.seed(pcg_extras::seed_seq_from<std::random_device>());
}

uint32_t RandomEngine::nextRandomInteger() {
    return randomState->pcg();
}

} // namespace common
} // namespace kuzu
