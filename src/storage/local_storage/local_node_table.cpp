#include "storage/local_storage/local_node_table.h"

#include "common/cast.h"
#include "storage/storage_utils.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void LocalNodeNG::scan(TableReadState& state) {
    KU_ASSERT(state.columnIDs.size() == state.outputVectors.size());
    for (auto i = 0u; i < state.columnIDs.size(); i++) {
        auto columnID = state.columnIDs[i];
        for (auto pos = 0u; pos < state.nodeIDVector.state->selVector->selectedSize; pos++) {
            auto nodeIDPos = state.nodeIDVector.state->selVector->selectedPositions[pos];
            auto nodeOffset = state.nodeIDVector.getValue<nodeID_t>(nodeIDPos).offset;
            auto posInOutputVector =
                state.outputVectors[i]->state->selVector->selectedPositions[pos];
            lookupInternal(nodeOffset, columnID, *state.outputVectors[i], posInOutputVector);
        }
    }
}

bool LocalNodeNG::hasUpdatesOrInserts() const {
    return LocalNodeGroup::hasUpdatesOrInserts(updateInfos, insertInfo);
}

bool LocalNodeNG::hasUpdatesOrDeletions() const {
    return LocalNodeGroup::hasUpdatesOrDeletions(updateInfos, deleteInfo);
}

void LocalNodeNG::lookupInternal(common::offset_t nodeOffset, common::column_id_t columnID,
    common::ValueVector& output, common::sel_t posInOutputVector) {
    if (deleteInfo.deletedOffsets.contains(nodeOffset)) {
        // Node has been deleted.
        return;
    }
    if (updateInfos[columnID].offsetToRowIdx.contains(nodeOffset)) {
        // Node has been updated.
        auto rowIdx = updateInfos[columnID].offsetToRowIdx.at(nodeOffset);
        auto [chunkIdx, posInChunk] =
            ChunkedNodeGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        updateChunksPerColumn[columnID]->getChunkedGroup(chunkIdx)->getColumnChunk(columnID).lookup(
            posInChunk, output, posInOutputVector);
        return;
    }
    if (insertInfo.offsetToRowIdx.contains(nodeOffset)) {
        // Node has been inserted.
        auto rowIdx = insertInfo.offsetToRowIdx.at(nodeOffset);
        auto [chunkIdx, posInChunk] =
            ChunkedNodeGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        insertChunks->getChunkedGroup(chunkIdx)->getColumnChunk(columnID).lookup(
            posInChunk, output, posInOutputVector);
    }
}

// TODO(Guodong): Abstract filterNull logic.

bool LocalNodeNG::insert(TableInsertState& state) {
    auto& insertState = ku_dynamic_cast<TableInsertState&, NodeTableInsertState&>(state);
    auto& nodeIDVector = insertState.nodeIDVector;
    KU_ASSERT(nodeIDVector.state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector.state->selVector->selectedPositions[0];
    if (nodeIDVector.isNull(nodeIDPos)) {
        return false;
    }
    // The nodeOffset here should be the offset within the node group.
    auto nodeOffset = nodeIDVector.getValue<nodeID_t>(nodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(nodeOffset < StorageConstants::NODE_GROUP_SIZE);
    auto& rowIdxVector = state.localState.getRowIdxVector();
    KU_ASSERT(rowIdxVector.state->selVector->isUnfiltered());
    insertInfo.offsetToRowIdx[nodeOffset] =
        rowIdxVector.getValue<row_idx_t>(rowIdxVector.state->selVector->selectedPositions[0]);
    return true;
}

bool LocalNodeNG::update(TableUpdateState& state) {
    auto& updateState = ku_dynamic_cast<TableUpdateState&, NodeTableUpdateState&>(state);
    auto& nodeIDVector = updateState.nodeIDVector;
    KU_ASSERT(nodeIDVector.state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector.state->selVector->selectedPositions[0];
    if (nodeIDVector.isNull(nodeIDPos)) {
        return false;
    }
    auto nodeOffset = nodeIDVector.getValue<nodeID_t>(nodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(nodeOffset < StorageConstants::NODE_GROUP_SIZE);
    auto& rowIdxVector = state.localState.getRowIdxVector();
    KU_ASSERT(rowIdxVector.state->selVector->isUnfiltered());
    KU_ASSERT(updateState.columnID < updateInfos.size());
    updateInfos[updateState.columnID].offsetToRowIdx[nodeOffset] =
        rowIdxVector.getValue<row_idx_t>(rowIdxVector.state->selVector->selectedPositions[0]);
    return true;
}

bool LocalNodeNG::delete_(TableDeleteState& state) {
    auto& deleteState = ku_dynamic_cast<TableDeleteState&, NodeTableDeleteState&>(state);
    auto& nodeIDVector = deleteState.nodeIDVector;
    auto nodeIDPos = nodeIDVector.state->selVector->selectedPositions[0];
    if (nodeIDVector.isNull(nodeIDPos)) {
        return false;
    }
    auto nodeOffset = nodeIDVector.getValue<nodeID_t>(nodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(nodeOffset < StorageConstants::NODE_GROUP_SIZE);
    deleteInfo.deletedOffsets.insert(nodeOffset);
    return true;
}

void LocalNodeNG::prepareCommit(transaction::Transaction* transaction,
    common::node_group_idx_t nodeGroupIdx, TableData& tableData) {
    KU_ASSERT(tableData.getNumColumns() == updateChunksPerColumn.size());
    auto& nodeTableData = ku_dynamic_cast<TableData&, NodeTableData&>(tableData);
    ChunkCollection insertChunkCollection;
    ChunkCollection updateChunkCollection;
    for (auto columnID = 0u; columnID < nodeTableData.getNumColumns(); columnID++) {
        auto column = nodeTableData.getColumn(columnID);
        for (auto chunkIdx = 0u; chunkIdx < insertChunks->getNumChunkedGroups(); chunkIdx++) {
            auto& chunk = insertChunks->getChunkedGroup(chunkIdx)->getColumnChunk(columnID);
            insertChunkCollection.push_back(&chunk);
        }
        KU_ASSERT(columnID < updateChunksPerColumn.size());
        auto& updateChunks = updateChunksPerColumn[columnID];
        for (auto chunkIdx = 0u; chunkIdx < updateChunks->getNumChunkedGroups(); chunkIdx++) {
            auto& chunk = updateChunks->getChunkedGroup(chunkIdx)->getColumnChunk(columnID);
            updateChunkCollection.push_back(&chunk);
        }
        column->prepareCommitForChunk(transaction, nodeGroupIdx, insertChunkCollection,
            insertInfo.offsetToRowIdx, updateChunkCollection, updateInfos[columnID].offsetToRowIdx,
            {} /*deleteInfo*/);
    }
}

LocalNodeTable::LocalNodeTable(Table& table, WAL& wal) : LocalTable{table, wal} {
    std::vector<LogicalType> types;
    types.reserve(table.getNumColumns());
    for (auto i = 0u; i < table.getNumColumns(); i++) {
        types.push_back(table.getColumnType(i));
    }
    insertChunks = std::make_shared<ChunkedNodeGroupCollection>(types);
    updateChunks.reserve(types.size());
    for (auto i = 0u; i < types.size(); i++) {
        std::vector<LogicalType> chunkCollectionTypes;
        chunkCollectionTypes.push_back(types[i]);
        updateChunks.push_back(std::make_shared<ChunkedNodeGroupCollection>(chunkCollectionTypes));
    }
}

bool LocalNodeTable::insert(TableInsertState& state) {
    auto& insertState = ku_dynamic_cast<TableInsertState&, NodeTableInsertState&>(state);
    insertChunks->append(insertState.propertyVectors, state.localState.getRowIdxVectorUnsafe());
    return getLocalNodeGroup(insertState.nodeIDVector, NotExistAction::CREATE)->insert(state);
}

bool LocalNodeTable::update(TableUpdateState& state) {
    auto& updateState = ku_dynamic_cast<TableUpdateState&, NodeTableUpdateState&>(state);
    KU_ASSERT(updateState.columnID < updateChunks.size());
    std::vector<ValueVector*> propertyVectors = {
        const_cast<ValueVector*>(&updateState.propertyVector)};
    updateChunks[updateState.columnID]->append(
        propertyVectors, updateState.localState.getRowIdxVectorUnsafe());
    return getLocalNodeGroup(updateState.nodeIDVector, NotExistAction::CREATE)->update(state);
}

bool LocalNodeTable::delete_(TableDeleteState& state) {
    auto& deleteState = ku_dynamic_cast<TableDeleteState&, NodeTableDeleteState&>(state);
    return getLocalNodeGroup(deleteState.nodeIDVector, NotExistAction::CREATE)->delete_(state);
}

void LocalNodeTable::scan(TableReadState& state) {
    auto localNodeGroup = getLocalNodeGroup(state.nodeIDVector);
    if (localNodeGroup) {
        localNodeGroup->scan(state);
    }
}

void LocalNodeTable::lookup(TableReadState& state) {
    auto localNodeGroup = getLocalNodeGroup(state.nodeIDVector);
    if (localNodeGroup) {
        localNodeGroup->scan(state);
    }
}

LocalNodeGroup* LocalNodeTable::getLocalNodeGroup(
    const ValueVector& nodeIDVector, NotExistAction action) {
    auto nodeIDPos = nodeIDVector.state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector.getValue<common::nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!localNodeGroups.contains(nodeGroupIdx)) {
        if (action == NotExistAction::CREATE) {
            localNodeGroups[nodeGroupIdx] = createLocalNodeGroup(nodeGroupIdx);
        } else {
            return nullptr;
        }
    }
    KU_ASSERT(localNodeGroups.contains(nodeGroupIdx));
    return localNodeGroups.at(nodeGroupIdx).get();
}

std::unique_ptr<LocalNodeGroup> LocalNodeTable::createLocalNodeGroup(
    node_group_idx_t nodeGroupIdx) {
    return std::make_unique<LocalNodeNG>(
        insertChunks, updateChunks, StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx));
}

void LocalNodeTable::prepareCommit(transaction::Transaction* transaction) {
    // TODO: Prepare commit for hash index.
    auto& tableData = ku_dynamic_cast<Table&, NodeTable&>(table).getTableDataUnsafe();
    for (auto& [nodeGroupIdx, localNodeGroup] : localNodeGroups) {
        if (!localNodeGroup->hasUpdatesOrInserts()) {
            continue;
        }
        localNodeGroup->prepareCommit(transaction, nodeGroupIdx, tableData);
    }
    table.prepareCommit(transaction, this);
    wal.addToUpdatedTables(table.getTableID());
}

void LocalNodeTable::prepareRollback(transaction::Transaction*) {
    localNodeGroups.clear();
    table.prepareRollback(this);
}

} // namespace storage
} // namespace kuzu
