#include "processor/operator/scan/scan_node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool ScanSingleNodeTable::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    table->scan(transaction, inputNodeIDVector, propertyColumnIds, outPropertyVectors);
    return true;
}

bool ScanMultiNodeTables::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    auto tableID =
        inputNodeIDVector
            ->getValue<nodeID_t>(inputNodeIDVector->state->selVector->selectedPositions[0])
            .tableID;
    tables.at(tableID)->scan(
        transaction, inputNodeIDVector, tableIDToScanColumnIds.at(tableID), outPropertyVectors);
    return true;
}

} // namespace processor
} // namespace kuzu