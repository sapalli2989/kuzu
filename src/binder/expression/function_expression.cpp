#include "binder/expression/function_expression.h"

#include "binder/expression/expression_util.h"
#include "function/cast/vector_cast_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

std::string ScalarFunctionExpression::getUniqueName(const std::string& functionName,
    const kuzu::binder::expression_vector& children) {
    return stringFormat("({})", ExpressionUtil::toString(children));;
}

std::string ScalarFunctionExpression::toStringInternal() const {
    auto result = function.name + "(";
    result += ExpressionUtil::toString(children);
    if (functionName == "CAST") {
        result += ", ";
        result += bindData->resultType->toString();
    }
    result += ")";


    if (function.name == function::CastAnyFunction::name) {
        return stringFormat("{}({})", ExpressionUtil::toString(children));;
    }

    return stringFormat("{}({})", function.name,  ExpressionUtil::toString(children));;
}

std::string AggregateFunctionExpression::getUniqueName(const std::string& functionName,
    kuzu::binder::expression_vector& children, bool isDistinct) {
    auto result = functionName + "(";
    if (isDistinct) {
        result += "DISTINCT ";
    }
    for (auto& child : children) {
        result += child->getUniqueName() + ", ";
    }
    result += ")";
    return stringFormat("({})", ExpressionUtil::toString(children));;
}

std::string AggregateFunctionExpression::toStringInternal() const {
    auto result = functionName + "(";
    if (isDistinct()) {
        result += "DISTINCT ";
    }
    result += ExpressionUtil::toString(children);
    result += ")";
    return result;
}

} // namespace binder
} // namespace kuzu
