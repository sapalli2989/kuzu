#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct DecimalFunction {

    static std::unique_ptr<FunctionBindData> bindAddFunc(const binder::expression_vector& arguments,
        Function*);

    static std::unique_ptr<FunctionBindData> bindSubtractFunc(
        const binder::expression_vector& arguments, Function*);

    static std::unique_ptr<FunctionBindData> bindMultiplyFunc(
        const binder::expression_vector& arguments, Function*);

    static std::unique_ptr<FunctionBindData> bindDivideFunc(
        const binder::expression_vector& arguments, Function*);

    static std::unique_ptr<FunctionBindData> bindModuloFunc(
        const binder::expression_vector& arguments, Function*);
    
    static std::unique_ptr<FunctionBindData> bindNegateFunc(
        const binder::expression_vector& arguments, Function*);
    
    static std::unique_ptr<FunctionBindData> bindAbsFunc(
        const binder::expression_vector& arguments, Function*);
    
    static std::unique_ptr<FunctionBindData> bindFloorFunc(
        const binder::expression_vector& arguments, Function*);
    
    static std::unique_ptr<FunctionBindData> bindCeilFunc(
        const binder::expression_vector& arguments, Function*);
};

} // namespace function
} // namespace kuzu
