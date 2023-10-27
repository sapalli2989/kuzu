#pragma once

#include "common/vector/value_vector.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct MapCreationFunctions {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct MapExtractFunctions {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct MapKeysFunctions {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct MapValuesFunctions {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

} // namespace function
} // namespace kuzu
