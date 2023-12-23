#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Load final : public PhysicalOperator {
public:
    Load(std::string path, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::LOAD, id, paramsString}, path{std::move(path)},
          hasExecuted{false} {}

    inline bool isSource() const override { return true; }
    inline bool canParallel() const override { return false; }

    inline void initLocalStateInternal(
        ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) override {
        hasExecuted = false;
    }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<Load>(path, id, paramsString);
    }

private:
    std::string path;
    bool hasExecuted;
};

} // namespace processor
} // namespace kuzu
