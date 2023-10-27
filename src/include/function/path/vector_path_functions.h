#pragma once

#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct NodesVectorFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct RelsVectorFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct PropertiesBindData : public FunctionBindData {
    common::vector_idx_t childIdx;

    PropertiesBindData(common::LogicalType dataType, common::vector_idx_t childIdx)
        : FunctionBindData{std::move(dataType)}, childIdx{childIdx} {}
};

struct PropertiesVectorFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
    static void compileFunc(FunctionBindData* bindData,
        const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        std::shared_ptr<common::ValueVector>& result);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
};

struct IsTrailVectorFunction {
    static function_set getFunctionSet();
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
    static bool selectFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::SelectionVector& selectionVector);
};

struct IsACyclicVectorFunction {
    static function_set getFunctionSet();
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
    static bool selectFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::SelectionVector& selectionVector);
};

} // namespace function
} // namespace kuzu
