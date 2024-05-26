#include "storage/store/node_group.h"

#include "storage/store/node_table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

row_idx_t NodeGroup::getNumRows() const {
    const auto numRows = chunks[0]->getNumValues();
    for (auto i = 1u; i < chunks.size(); i++) {
        KU_ASSERT(numRows == chunks[i]->getNumValues());
    }
    return numRows;
}

void NodeGroup::scan(Transaction* transaction, const TableScanState& scanState,
    ValueVector& nodeIDVector, const std::vector<ValueVector*>& outputVectors) const {
    auto& nodeScanState = ku_dynamic_cast<const TableDataScanState&, const NodeDataScanState&>(
        *scanState.dataScanState);
    for (auto i = 0u; i < scanState.columnIDs.size(); i++) {
        if (scanState.columnIDs[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setAllNull();
        } else {
            KU_ASSERT(scanState.columnIDs[i] < chunks.size());
            chunks[scanState.columnIDs[i]]->scan(transaction, nodeScanState.chunkStates[i],
                nodeScanState.vectorIdx, nodeScanState.numRowsToScan, nodeIDVector,
                *outputVectors[i]);
        }
    }
}

void NodeGroup::lookup(Transaction* transaction, TableScanState& scanState,
    ValueVector& nodeIDVector, const std::vector<ValueVector*>& outputVectors) {}

} // namespace storage
} // namespace kuzu
