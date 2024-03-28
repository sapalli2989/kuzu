#include "processor/operator/recursive_extend/ife.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void Frontier::insert(
    common::nodeID_t srcNodeID, common::relID_t edgeID, common::nodeID_t dstNodeID) {
    if (!edgeInfosMap.contains(dstNodeID)) {
        nodeIDs.push_back(dstNodeID);
        std::vector<edge_info_t> edgeInfos;
        edgeInfos.emplace_back(srcNodeID, edgeID);
        edgeInfosMap.insert({dstNodeID, std::move(edgeInfos)});
        return;
    }
    auto& edgeInfos = edgeInfosMap.at(dstNodeID);
    edgeInfos.emplace_back(srcNodeID, edgeID);
}

void Frontier::merge(const Frontier& other) {
    for (auto& [otherNodeID, otherEdgeInfos] : other.edgeInfosMap) {
        if (!edgeInfosMap.contains(otherNodeID)) {
            auto edgeInfos = std::move(other.edgeInfosMap.at(otherNodeID));
            edgeInfosMap.insert({otherNodeID, std::move(edgeInfos)});
            continue;
        }
        auto& edgeInfos = edgeInfosMap.at(otherNodeID);
        for (auto& edgeInfo : otherEdgeInfos) {
            edgeInfos.push_back(std::move(edgeInfo));
        }
    }
}

FrontierMorsel IFESourceState::getFrontierMorsel() {
    std::unique_lock lck{mtx};
    KU_ASSERT(currentLevel < graph.frontiers.size());
    auto currentFrontier = &graph.frontiers[currentLevel];
    if (currentFrontierCursor == currentFrontier->size()) {
        return FrontierMorsel(nullptr, INVALID_OFFSET);
    }
    return FrontierMorsel{currentFrontier, currentFrontierCursor++};
}

void IFESourceState::finalizeFrontier(const Frontier& frontier) {
    std::unique_lock lck{mtx};
    numComputeThreadsFinished++;
    if (numComputeThreadsFinished == 0) { //

    } else if (numComputeThreadsFinished == numComputeThreadsRegistered) { //

    }
    // release lck
    // spin and wait for other threads

}

IFESourceState* IFESharedState::getSourceState() {
    std::unique_lock lck{mtx};
    if (tableCursor < inputTable->getNumTuples()) {
        auto newState = std::make_shared<IFESourceState>();
        auto newStatePtr = newState.get();
        activeSourceStates.push_back(std::move(newState));
        return newStatePtr;
    }
    IFESourceState* leastRegisteredState = nullptr;
    uint32_t leastThreadNum = UINT32_MAX;
    for (auto& state : activeSourceStates) {
        if (state->numComputeThreadsRegistered < leastThreadNum) {
            leastRegisteredState = state.get();
            leastThreadNum = state->numComputeThreadsRegistered;
        }
    }
    return leastRegisteredState;
}

void IFE::initLocalStateInternal(ResultSet*, ExecutionContext*) {}

bool IFE::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        // First we acquire a source state to work on.
        if (activeSourceState == nullptr) {
            activeSourceState = sharedState->getSourceState();
            if (activeSourceState == nullptr) { // No more
                return false;
            }
        }
        KU_ASSERT(activeSourceState != nullptr);
        if (activeSourceState->isRJGraphComplete()) {
            // Start parallel scanning
        }
        // Otherwise parallel compute a graph
        computeRJGraph(context);
    }
}

static void updatedFrontier(common::nodeID_t srcNodeID, ValueVector* nodeIDVector,
    ValueVector* edgeIDVector, Frontier& frontier) {
    auto selVector = nodeIDVector->state->selVector.get();
    for (auto i = 0u; i < selVector->selectedSize; ++i) {
        auto pos = selVector->selectedPositions[i];
        auto dstNodeID = nodeIDVector->getValue<nodeID_t>(pos);
        auto edgeID = edgeIDVector->getValue<relID_t>(pos);
        frontier.insert(srcNodeID, edgeID, dstNodeID);
    }
}

void IFE::computeRJGraph(ExecutionContext* context) {
    activeSourceState->registerNewThread();
    while (!activeSourceState->isRJGraphComplete()) {
        auto frontierMorsel = activeSourceState->getFrontierMorsel();
        if (frontierMorsel.isEmpty()) {
            //
        }
        // Compute next frontier locally.
        auto srcNodeID = frontierMorsel.frontier->getNodeID(frontierMorsel.offset);
        ifeScan->setNodeID(srcNodeID);
        Frontier localFrontier;
        while (ifePipelineRoot->getNextTuple(context)) {
            updatedFrontier(srcNodeID, vectors.recursiveDstNodeIDVector,
                vectors.recursiveEdgeIDVector, localFrontier);
        }
    }
}

void IFEScan::setNodeID(common::nodeID_t nodeID) {
    idVector->setValue<nodeID_t>(0, nodeID);
    executed = false;
}

void IFEScan::initLocalStateInternal(ResultSet* resultSet, ExecutionContext*) {
    idVector = resultSet->getValueVector(idPos).get();
}

bool IFEScan::getNextTuplesInternal(ExecutionContext*) {
    if (executed) {
        return false;
    }
    executed = true;
    return true;
}

} // namespace processor
} // namespace kuzu
