#pragma once

#include "planner/operator/logical_operator.h"
#include "transaction/transaction_action.h"

namespace kuzu {
namespace planner {

class LogicalLoad final : public LogicalOperator {
public:
    explicit LogicalLoad(std::string path)
        : LogicalOperator{LogicalOperatorType::LOAD}, path{std::move(path)} {}

    inline std::string getExpressionsForPrinting() const override { return path; }

    inline void computeFlatSchema() override { createEmptySchema(); }
    inline void computeFactorizedSchema() override { createEmptySchema(); }

    inline std::string getPath() { return path; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalLoad>(path);
    }

private:
    std::string path;
};

} // namespace planner
} // namespace kuzu
