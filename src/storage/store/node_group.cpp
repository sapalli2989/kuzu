#include "storage/store/node_group.h"

#include "storage/store/table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void NodeGroup::initializeScanState(Transaction*, TableScanState& state) const {
    auto& nodeGroupState = state.nodeGroupScanState;
    // TODO: Should handle transaction to resolve version visibility here.
    nodeGroupState.maxNumRowsToScan = getNumRows();
}

void NodeGroup::scan(Transaction*, TableScanState& state) const {
    // TODO: Should handle transaction to resolve version visibility here.
    KU_ASSERT(state.source == TableScanSource::COMMITTED);
    auto& nodeGroupState = state.nodeGroupScanState;
    KU_ASSERT(nodeGroupState.chunkedGroupIdx < chunkedGroups.getNumChunkedGroups());
    KU_ASSERT(nodeGroupState.nextRowToScan < nodeGroupState.maxNumRowsToScan);
    auto& chunkedGroup = chunkedGroups.getChunkedGroup(nodeGroupState.chunkedGroupIdx);
    if (nodeGroupState.nextRowToScan ==
        chunkedGroup.getStartNodeOffset() + chunkedGroup.getNumRows()) {
        nodeGroupState.chunkedGroupIdx++;
    }
    if (nodeGroupState.chunkedGroupIdx >= chunkedGroups.getNumChunkedGroups()) {
        state.nodeIDVector->state->getSelVectorUnsafe().setToUnfiltered(0);
        return;
    }
    auto& chunkedGroupToScan = chunkedGroups.getChunkedGroup(nodeGroupState.chunkedGroupIdx);
    const auto offsetToScan =
        nodeGroupState.nextRowToScan - chunkedGroupToScan.getStartNodeOffset();
    KU_ASSERT(offsetToScan < chunkedGroupToScan.getNumRows());
    const auto numRowsToScan =
        std::min(chunkedGroupToScan.getNumRows() - offsetToScan, DEFAULT_VECTOR_CAPACITY);
    // TODO: We should switch on if the node group is in-memory or on-disk to call different scan
    // functions here.
    chunkedGroupToScan.scan(state.columnIDs, state.outputVectors, offsetToScan, numRowsToScan);
    const auto nodeOffset = startNodeOffset + nodeGroupState.nextRowToScan;
    for (auto i = 0u; i < numRowsToScan; i++) {
        state.nodeIDVector->setValue<nodeID_t>(i, {nodeOffset + i, state.tableID});
    }
    state.nodeIDVector->state->getSelVectorUnsafe().setToUnfiltered(numRowsToScan);

    nodeGroupState.nextRowToScan += numRowsToScan;
}

} // namespace storage
} // namespace kuzu
