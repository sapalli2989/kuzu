#pragma once

#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct OctetLengthFunctions : public VectorFunction {
    static function_set getFunctionSet();
};

struct EncodeFunctions : public VectorFunction {
    static function_set getFunctionSet();
};

struct DecodeFunctions : public VectorFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
