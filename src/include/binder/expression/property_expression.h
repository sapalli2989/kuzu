#pragma once

#include "common/constants.h"
#include "common/keyword/rdf_keyword.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class PropertyExpression : public Expression {
public:
    PropertyExpression(common::LogicalType dataType, const std::string& propertyName,
        const std::string& uniqueVarName, const std::string& varName,
        common::table_id_map_t<common::property_id_t> propertyIDPerTable, bool isPrimaryKey_)
        : Expression{common::ExpressionType::PROPERTY, std::move(dataType),
              getUniqueName(uniqueVarName, propertyName)},
          isPrimaryKey_{isPrimaryKey_}, propertyName{propertyName}, uniqueVarName{uniqueVarName},
          varName{varName}, propertyIDPerTable{std::move(propertyIDPerTable)} {}
    PropertyExpression(const PropertyExpression& other)
        : Expression{common::ExpressionType::PROPERTY, other.dataType, other.uniqueName},
          isPrimaryKey_{other.isPrimaryKey_}, propertyName{other.propertyName},
          uniqueVarName{other.uniqueVarName}, varName{other.varName},
          propertyIDPerTable{other.propertyIDPerTable} {}

    // Construct from a property that is stored physically.
    static std::unique_ptr<PropertyExpression> construct(common::LogicalType type,
        const std::string& propertyName, const Expression& pattern,
        const common::table_id_map_t<common::property_id_t>& propertyIDPerTable, bool isPrimaryKey);
    // Construct from a virtual property, i.e. no propertyID available.
    static std::unique_ptr<PropertyExpression> construct(common::LogicalType type,
        const std::string& propertyName, const Expression& pattern);

    bool isPrimaryKey() const { return isPrimaryKey_; }
    std::string getPropertyName() const { return propertyName; }
    // TODO: rename me
    std::string getVariableName() const { return uniqueVarName; }

    bool hasPropertyID(common::table_id_t tableID) const {
        return propertyIDPerTable.contains(tableID);
    }
    common::property_id_t getPropertyID(common::table_id_t tableID) const {
        KU_ASSERT(propertyIDPerTable.contains(tableID));
        return propertyIDPerTable.at(tableID);
    }

    bool isInternalID() const { return getPropertyName() == common::InternalKeyword::ID; }
    bool isIRI() const { return getPropertyName() == common::rdf::IRI; }

    std::unique_ptr<Expression> copy() const override {
        return std::make_unique<PropertyExpression>(*this);
    }

private:
    std::string toStringInternal() const final { return varName + "." + propertyName; }

    static std::string getUniqueName(const std::string& uniqueVarName,
        const std::string& propertyName) {
        return uniqueVarName + "." + propertyName;
    }

private:
    bool isPrimaryKey_ = false;
    std::string propertyName;
    // unique identifier references to a node/rel table.
    std::string uniqueVarName;
    // printable identifier references to a node/rel table.
    std::string varName;
    common::table_id_map_t<common::property_id_t> propertyIDPerTable;
};

} // namespace binder
} // namespace kuzu
