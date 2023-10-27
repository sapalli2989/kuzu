#include "function/schema/vector_node_rel_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

function_set OffsetVectorFunction::getFunctionSet() {
    function_set definitions;
    definitions.push_back(make_unique<ScalarFunction>(OFFSET_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERNAL_ID}, LogicalTypeID::INT64,
        OffsetVectorFunction::execFunction));
    return definitions;
}

} // namespace function
} // namespace kuzu
