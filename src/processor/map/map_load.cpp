#include "planner/operator/logical_load.h"
#include "processor/operator/load.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLoad(LogicalOperator* logicalOperator) {
    auto logicalLoad = reinterpret_cast<LogicalLoad*>(logicalOperator);
    return std::make_unique<Load>(
        logicalLoad->getPath(), getOperatorID(), logicalLoad->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
