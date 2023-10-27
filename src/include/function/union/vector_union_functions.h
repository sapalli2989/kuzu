#pragma once

#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct UnionValueVectorFunction : public VectorFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
    static void compileFunc(FunctionBindData* bindData,
        const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        std::shared_ptr<common::ValueVector>& result);
};

struct UnionTagVectorFunction : public VectorFunction {
    static function_set getFunctionSet();
};

struct UnionExtractVectorFunction : public VectorFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
