#include "storage/local_storage/local_node_table.h"

#include "common/cast.h"
#include "storage/storage_utils.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

// void LocalNodeNG::initializeScanState(TableScanState& scanState) const {
//     auto& dataScanState =
//         ku_dynamic_cast<TableDataScanState&, NodeDataScanState&>(*scanState.dataScanState);
//     dataScanState.vectorIdx = INVALID_VECTOR_IDX;
//     dataScanState.numRowsInNodeGroup = insertChunks.getOffsetToRowIdx().size();
// }
//
// void LocalNodeNG::scan(TableScanState& scanState) {
//     const auto& dataScanState =
//         ku_dynamic_cast<TableDataScanState&, NodeDataScanState&>(*scanState.dataScanState);
//     const auto startIdx = dataScanState.vectorIdx * DEFAULT_VECTOR_CAPACITY;
//     auto itr = insertChunks.getOffsetToRowIdx().begin();
//     for (auto i = 0u; i < startIdx; i++) {
//         ++itr;
//     }
//     for (auto i = 0u; i < dataScanState.numRowsToScan; i++) {
//         KU_ASSERT(itr != insertChunks.getOffsetToRowIdx().end());
//         const auto offset = itr->first;
//         scanState.nodeIDVector->setValue<internalID_t>(i, {offset + nodeGroupStartOffset,
//         tableID}); KU_ASSERT(!deleteInfo.containsOffset(offset)); insertChunks.read(offset,
//         scanState.columnIDs, scanState.outputVectors, i);
//         ++itr;
//     }
//     scanState.nodeIDVector->state->getSelVectorUnsafe().setToUnfiltered(
//         dataScanState.numRowsToScan);
// }
//
// void LocalNodeNG::lookup(const ValueVector& nodeIDVector, const std::vector<column_id_t>&
// columnIDs,
//     const std::vector<ValueVector*>& outputVectors) {
//     for (auto i = 0u; i < nodeIDVector.state->getSelVector().getSelSize(); i++) {
//         const auto nodeIDPos = nodeIDVector.state->getSelVector()[i];
//         const auto nodeOffset = nodeIDVector.getValue<nodeID_t>(nodeIDPos).offset;
//         if (deleteInfo.containsOffset(nodeOffset)) {
//             // Node has been deleted.
//             return;
//         }
//         for (auto columnIdx = 0u; columnIdx < columnIDs.size(); columnIdx++) {
//             const auto posInOutputVector = outputVectors[columnIdx]->state->getSelVector()[i];
//             if (columnIDs[columnIdx] == INVALID_COLUMN_ID) {
//                 outputVectors[columnIdx]->setNull(posInOutputVector, true);
//                 continue;
//             }
//             getUpdateChunks(columnIDs[columnIdx])
//                 .read(nodeOffset, 0 /*columnID*/, outputVectors[columnIdx], posInOutputVector);
//         }
//     }
// }
//
// bool LocalNodeNG::insert(std::vector<ValueVector*> nodeIDVectors,
//     std::vector<ValueVector*> propertyVectors) {
//     KU_ASSERT(nodeIDVectors.size() == 1);
//     const auto nodeIDVector = nodeIDVectors[0];
//     KU_ASSERT(nodeIDVector->state->getSelVector().getSelSize() == 1);
//     const auto nodeIDPos = nodeIDVector->state->getSelVector()[0];
//     if (nodeIDVector->isNull(nodeIDPos)) {
//         return false;
//     }
//     // The nodeOffset here should be the offset within the node group.
//     const auto nodeOffset =
//         nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset - nodeGroupStartOffset;
//     KU_ASSERT(nodeOffset < StorageConstants::NODE_GROUP_SIZE);
//     if (deleteInfo.containsOffset(nodeOffset)) {
//         deleteInfo.clearNodeOffset(nodeOffset);
//     }
//     insertChunks.append(nodeOffset, propertyVectors);
//     return true;
// }
//
// bool LocalNodeNG::update(std::vector<ValueVector*> nodeIDVectors, column_id_t columnID,
//     ValueVector* propertyVector) {
//     KU_ASSERT(nodeIDVectors.size() == 1);
//     const auto nodeIDVector = nodeIDVectors[0];
//     KU_ASSERT(nodeIDVector->state->getSelVector().getSelSize() == 1);
//     const auto nodeIDPos = nodeIDVector->state->getSelVector()[0];
//     if (nodeIDVector->isNull(nodeIDPos)) {
//         return false;
//     }
//     const auto nodeOffset =
//         nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset - nodeGroupStartOffset;
//     KU_ASSERT(nodeOffset < StorageConstants::NODE_GROUP_SIZE && columnID < updateChunks.size());
//     // Check if the node is newly inserted or in persistent storage.
//     if (insertChunks.hasOffset(nodeOffset)) {
//         insertChunks.update(nodeOffset, columnID, propertyVector);
//     } else {
//         updateChunks[columnID].append(nodeOffset, {propertyVector});
//     }
//     return true;
// }
//
// bool LocalNodeNG::delete_(ValueVector* nodeIDVector, ValueVector*) {
//     KU_ASSERT(nodeIDVector->state->getSelVector().getSelSize() == 1);
//     const auto nodeIDPos = nodeIDVector->state->getSelVector()[0];
//     if (nodeIDVector->isNull(nodeIDPos)) {
//         return false;
//     }
//     const auto nodeOffset =
//         nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset - nodeGroupStartOffset;
//     KU_ASSERT(nodeOffset < StorageConstants::NODE_GROUP_SIZE);
//     // Check if the node is newly inserted or in persistent storage.
//     if (insertChunks.hasOffset(nodeOffset)) {
//         insertChunks.remove(nodeOffset);
//     } else {
//         for (auto i = 0u; i < updateChunks.size(); i++) {
//             updateChunks[i].remove(nodeOffset);
//         }
//         deleteInfo.deleteOffset(nodeOffset);
//     }
//     return true;
// }
//
// LocalNodeGroup* LocalNodeTableData::getOrCreateLocalNodeGroup(ValueVector* nodeIDVector) {
//     const auto nodeIDPos = nodeIDVector->state->getSelVector()[0];
//     const auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
//     const auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
//     if (!nodeGroups.contains(nodeGroupIdx)) {
//         auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
//         nodeGroups[nodeGroupIdx] =
//             std::make_unique<LocalNodeNG>(tableID, nodeGroupStartOffset, dataTypes);
//     }
//     return nodeGroups.at(nodeGroupIdx).get();
// }
//
static std::vector<LogicalType> getTableColumnTypes(const NodeTable& table) {
    std::vector<LogicalType> types;
    for (auto i = 0u; i < table.getNumColumns(); i++) {
        types.push_back(table.getColumn(i)->getDataType());
    }
    return types;
}

LocalNodeTable::LocalNodeTable(Table& table)
    : LocalTable{table},
      chunkedGroups{getTableColumnTypes(ku_dynamic_cast<const Table&, const NodeTable&>(table))} {}

bool LocalNodeTable::insert(TableInsertState& insertState) {
    // TODO(Guodong): Assume all property vectors have the same selVector here. Should be changed.
    // TODO(Guodong): Should check local hash index here.
    chunkedGroups.append(insertState.propertyVectors,
        insertState.propertyVectors[0]->state->getSelVector());
    return true;
}

bool LocalNodeTable::update(TableUpdateState& state) {
    const auto& updateState = ku_dynamic_cast<TableUpdateState&, NodeTableUpdateState&>(state);
}

bool LocalNodeTable::delete_(TableDeleteState& deleteState) {
    const auto& deleteState_ =
        ku_dynamic_cast<TableDeleteState&, NodeTableDeleteState&>(deleteState);
}

void LocalNodeTable::initializeScanState(TableScanState& scanState) const {
    auto& dataScanState =
        ku_dynamic_cast<TableDataScanState&, NodeDataScanState&>(*scanState.dataScanState);
    dataScanState.vectorIdx = INVALID_VECTOR_IDX;
    dataScanState.numRowsInNodeGroup = getNumRows();
}

void LocalNodeTable::scan(TableScanState& scanState) {
    KU_ASSERT(scanState.source == TableScanSource::UNCOMMITTED);
    // Fill node ID vector.
    auto& scanDataState =
        ku_dynamic_cast<TableDataScanState&, NodeDataScanState&>(*scanState.dataScanState);
    auto startNodeOffset = StorageConstants::MAX_NUM_NODES_IN_TABLE +
                           scanDataState.vectorIdx * DEFAULT_VECTOR_CAPACITY;
    for (auto i = 0u; i < scanDataState.numRowsToScan; i++) {
        scanState.nodeIDVector->setValue(i, nodeID_t{startNodeOffset + i, table.getTableID()});
    }
    auto& chunkedGroup = chunkedGroups.getChunkedGroup(scanDataState.vectorIdx);
    chunkedGroup.scan(scanState.columnIDs, scanState.outputVectors, 0, scanDataState.numRowsToScan);
    scanState.nodeIDVector->state->getSelVectorUnsafe().setSelSize(scanDataState.numRowsToScan);
}

row_idx_t LocalNodeTable::getNumRows() const {
    row_idx_t numRows = 0;
    for (auto& chunkedGroup : chunkedGroups.getChunkedGroups()) {
        numRows += chunkedGroup->getNumRows();
    }
    return numRows;
}

} // namespace storage
} // namespace kuzu
