#include "binder/expression/property_expression.h"

#include "binder/expression/node_rel_expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

std::unique_ptr<PropertyExpression> PropertyExpression::construct(LogicalType type,
    const std::string& propertyName, const Expression& child,
    const std::unordered_map<table_id_t, property_id_t>& propertyIDPerTable, bool isPrimaryKey) {
    KU_ASSERT(child.expressionType == ExpressionType::PATTERN);
    auto& patternExpr = child.constCast<NodeOrRelExpression>();
    auto variableName = patternExpr.getVariableName();
    auto uniqueName = patternExpr.getUniqueName();
    return std::make_unique<PropertyExpression>(type, propertyName, uniqueName, variableName,
        std::move(propertyIDPerTable), isPrimaryKey);
}

std::unique_ptr<PropertyExpression> PropertyExpression::construct(LogicalType type,
    const std::string& propertyName, const Expression& child) {
    KU_ASSERT(child.expressionType == ExpressionType::PATTERN);
    auto& patternExpr = child.constCast<NodeOrRelExpression>();
    auto variableName = patternExpr.getVariableName();
    auto uniqueName = patternExpr.getUniqueName();
    // Assign an invalid property id for virtual property.
    table_id_map_t<property_id_t> propertyIDPerTable;
    for (auto& tableID : patternExpr.getTableIDs()) {
        propertyIDPerTable.insert({tableID, INVALID_PROPERTY_ID});
    }
    return std::make_unique<PropertyExpression>(type, propertyName, uniqueName, variableName,
        std::move(propertyIDPerTable), false /* isPrimaryKey*/);
}

} // namespace binder
} // namespace kuzu
