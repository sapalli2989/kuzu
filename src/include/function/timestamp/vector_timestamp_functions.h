#pragma once

#include "function/scalar_function.h"

namespace kuzu {
namespace function {
class VectorTimestampFunction : public VectorFunction {};

struct CenturyVectorFunction : public VectorTimestampFunction {
    static function_set getFunctionSet();
};

struct EpochMsVectorFunction : public VectorTimestampFunction {
    static function_set getFunctionSet();
};

struct ToTimestampVectorFunction : public VectorTimestampFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
