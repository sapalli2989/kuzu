#include "function/algorithm/algorithm_function_collection.h"
#include "function/algorithm_function.h"

namespace kuzu {
namespace function {

struct ShortestPathExtraBindData final : public AlgoFuncExtraBindData {

};

function_set ShortestPathFunction::getFunctionSet() {
    function_set result;

    return result;
}

} // namespace function
} // namespace kuzu
