#pragma once

#include "function/scalar_function.h"

namespace kuzu {
namespace function {
class VectorDateFunction : public VectorFunction {};

struct DatePartVectorFunction : public VectorDateFunction {
    static function_set getFunctionSet();
};

struct DateTruncVectorFunction : public VectorDateFunction {
    static function_set getFunctionSet();
};

struct DayNameVectorFunction : public VectorDateFunction {
    static function_set getFunctionSet();
};

struct GreatestVectorFunction : public VectorDateFunction {
    static function_set getFunctionSet();
};

struct LastDayVectorFunction : public VectorDateFunction {
    static function_set getFunctionSet();
};

struct LeastVectorFunction : public VectorDateFunction {
    static function_set getFunctionSet();
};

struct MakeDateVectorFunction : public VectorDateFunction {
    static function_set getFunctionSet();
};

struct MonthNameVectorFunction : public VectorDateFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
