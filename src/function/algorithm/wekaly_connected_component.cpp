#include "function/algorithm/algorithm_function_collection.h"
#include "function/algorithm_function.h"
#include "graph/graph.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

static AlgoFuncBindData bindFunc(const expression_vector& params) {
    auto bindData = AlgoFuncBindData();
    bindData.columnNames.push_back("node_id");
    bindData.columnNames.push_back("group_id");
    bindData.columnTypes.push_back(*LogicalType::INTERNAL_ID());
    bindData.columnTypes.push_back(*LogicalType::INT64());
    return bindData;
}

struct SourceState {

};

struct WeaklyConnectedComponentLocalState : public AlgoFuncLocalState {


};

static void dfs(graph::Graph* graph, std::vector<bool>& visitedMap) {

}

static void findConnectedComponent(graph::Graph* graph, std::vector<bool>& visitedMap, std::vector<int64_t>& groupID, common::offset_t offset, int64_t groupID) {
    visitedMap[offset] = true;

}

static void execFunc(const AlgoFuncInput& input, FactorizedTable& output) {
    auto graph = input.graph;
    auto localState = input.localState->ptrCast<WeaklyConnectedComponentLocalState>();

    std::vector<bool> visitedArray;
    std::vector<int64_t> groupIDArray;
    visitedMap.resize(graph->getNumNodes());
    for (auto offset = 0u; offset < graph->getNumNodes(); ++offset) {
        visitedMap[offset] = false;
    }

    auto groupID = 0;
    for (auto offset = 0u; offset < graph->getNumNodes(); ++offset) {
        if (visitedMap[offset]) {
            continue;
        }
        findConnectedComponent(graph, visitedMap, offset, groupID++);
    }
}

function_set WeaklyConnectedComponentFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<AlgorithmFunction>(name, std::vector<L>)
}

}
}