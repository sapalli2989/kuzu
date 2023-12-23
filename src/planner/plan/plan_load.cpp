#include "binder/bound_load_statement.h"
#include "common/cast.h"
#include "planner/operator/logical_load.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalPlan> Planner::planLoad(const BoundStatement& statement) {
    auto& loadStatement =
        common::ku_dynamic_cast<const BoundStatement&, const BoundLoadStatement&>(statement);
    auto logicalLoad = std::make_shared<LogicalLoad>(loadStatement.getPath());
    return getSimplePlan(std::move(logicalLoad));
}

} // namespace planner
} // namespace kuzu
