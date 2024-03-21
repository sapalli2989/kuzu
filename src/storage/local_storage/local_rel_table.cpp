#include "storage/local_storage/local_rel_table.h"

#include "storage/storage_utils.h"
#include "storage/store/rel_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

LocalRelNG::LocalRelNG(std::shared_ptr<ChunkedNodeGroupCollection> insertChunks,
    std::vector<std::shared_ptr<ChunkedNodeGroupCollection>> updateChunks,
    common::RelDataDirection direction, offset_t nodeGroupStartOffset)
    : LocalNodeGroup{insertChunks, updateChunks, nodeGroupStartOffset}, direction{direction} {}

void LocalRelNG::scan(TableReadState& state) {
    auto& relReadState =
        ku_dynamic_cast<TableDataReadState&, RelDataReadState&>(*state.dataReadState);
    auto offsetInChunk = relReadState.currentNodeOffset - relReadState.startNodeOffset;
    auto numValuesRead = scanCSR(
        offsetInChunk, relReadState.posInCurrentCSR, relReadState.columnIDs, state.outputVectors);
    relReadState.posInCurrentCSR += numValuesRead;
}

row_idx_t LocalRelNG::scanCSR(offset_t srcOffsetInChunk, offset_t posToReadForOffset,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(columnIDs.size() == outputVectors.size());
    std::vector<row_idx_t> rowIdxesToRead;
    rowIdxesToRead.reserve(DEFAULT_VECTOR_CAPACITY);
    auto& insertedRelOffsets = insertInfo.srcNodeOffsetToRelOffsets.at(srcOffsetInChunk);
    for (auto i = posToReadForOffset; i < insertedRelOffsets.size(); i++) {
        if (rowIdxesToRead.size() == DEFAULT_VECTOR_CAPACITY) {
            break;
        }
        rowIdxesToRead.push_back(insertInfo.offsetToRowIdx.at(insertedRelOffsets[i]));
    }
    for (auto i = 0u; i < columnIDs.size(); i++) {
        uint64_t posInOutputVector = 0;
        for (auto rowIdx : rowIdxesToRead) {
            auto [chunkIdx, offsetInChunk] =
                ChunkedNodeGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
            insertChunks->getChunkedGroup(chunkIdx)
                ->getColumnChunk(columnIDs[i])
                .lookup(offsetInChunk, *outputVectors[i], posInOutputVector++);
        }
    }
    auto numRelsRead = rowIdxesToRead.size();
    outputVectors[0]->state->selVector->resetSelectorToUnselectedWithSize(numRelsRead);
    return numRelsRead;
}

void LocalRelNG::applyLocalChangesToScannedVectors(offset_t srcOffset,
    const std::vector<column_id_t>& columnIDs, ValueVector* relIDVector,
    const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(columnIDs.size() == outputVectors.size());
    // Apply updates first, as applying deletions might change selected state.
    for (auto i = 0u; i < columnIDs.size(); ++i) {
        applyCSRUpdates(columnIDs[i], relIDVector, outputVectors[i]);
    }
    // Apply deletions and update selVector if necessary.
    applyCSRDeletions(srcOffset, relIDVector);
}

void LocalRelNG::applyCSRUpdates(
    column_id_t columnID, ValueVector* relIDVector, ValueVector* outputVector) {
    auto& updateInfo = updateInfos[columnID];
    for (auto i = 0u; i < relIDVector->state->selVector->selectedSize; i++) {
        auto pos = relIDVector->state->selVector->selectedPositions[i];
        KU_ASSERT(outputVector->state == relIDVector->state);
        auto relOffset = relIDVector->getValue<relID_t>(pos).offset;
        if (updateInfo.offsetToRowIdx.contains(relOffset)) {
            auto rowIdx = updateInfo.offsetToRowIdx.at(relOffset);
            auto [chunkIdx, offsetInChunk] =
                ChunkedNodeGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
            updateChunksPerColumn[columnID]
                ->getChunkedGroup(chunkIdx)
                ->getColumnChunk(columnID)
                .lookup(offsetInChunk, *outputVector, pos);
        }
    }
}

void LocalRelNG::applyCSRDeletions(offset_t srcOffset, ValueVector* relIDVector) {
    if (!deleteInfo.srcNodeOffsetToRelOffsetVec.contains(srcOffset)) {
        return;
    }
    auto selectPos = 0u;
    auto selVector = std::make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
    selVector->resetSelectorToValuePosBuffer();
    for (auto i = 0u; i < relIDVector->state->selVector->selectedSize; i++) {
        auto relIDPos = relIDVector->state->selVector->selectedPositions[i];
        auto relOffset = relIDVector->getValue<relID_t>(relIDPos).offset;
        if (deleteInfo.deletedOffsets.contains(relOffset)) {
            continue;
        }
        selVector->selectedPositions[selectPos++] = relIDPos;
    }
    if (selectPos != relIDVector->state->selVector->selectedSize) {
        relIDVector->state->selVector->resetSelectorToValuePosBuffer();
        memcpy(relIDVector->state->selVector->selectedPositions, selVector->selectedPositions,
            selectPos * sizeof(sel_t));
        relIDVector->state->selVector->selectedSize = selectPos;
    }
}

bool LocalRelNG::insert(TableInsertState& state) {
    auto& insertState = ku_dynamic_cast<TableInsertState&, RelTableInsertState&>(state);
    auto& boundNodeIDVector = direction == RelDataDirection::FWD ? insertState.srcNodeIDVector :
                                                                   insertState.dstNodeIDVector;
    KU_ASSERT(boundNodeIDVector.state->selVector->selectedSize == 1);
    auto boundNodeIDPos = boundNodeIDVector.state->selVector->selectedPositions[0];
    if (boundNodeIDVector.isNull(boundNodeIDPos)) {
        return false;
    }
    auto boundNodeOffset =
        boundNodeIDVector.getValue<nodeID_t>(boundNodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(boundNodeOffset < StorageConstants::NODE_GROUP_SIZE);
    auto relIDVector = insertState.propertyVectors[0];
    auto relIDPos = relIDVector->state->selVector->selectedPositions[0];
    auto relOffset = relIDVector->getValue<relID_t>(relIDPos).offset;
    auto& rowIdxVector = insertState.localState.getRowIdxVector();
    insertInfo.offsetToRowIdx[relOffset] =
        rowIdxVector.getValue<offset_t>(rowIdxVector.state->selVector->selectedPositions[0]);
    insertInfo.srcNodeOffsetToRelOffsets[boundNodeOffset].push_back(relOffset);
    return true;
}

bool LocalRelNG::update(TableUpdateState& state) {
    auto& updateState = ku_dynamic_cast<TableUpdateState&, RelTableUpdateState&>(state);
    auto& boundNodeIDVector = direction == RelDataDirection::FWD ? updateState.srcNodeIDVector :
                                                                   updateState.dstNodeIDVector;
    KU_ASSERT(boundNodeIDVector.state->selVector->selectedSize == 1);
    auto boundNodeIDPos = boundNodeIDVector.state->selVector->selectedPositions[0];
    if (boundNodeIDVector.isNull(boundNodeIDPos)) {
        return false;
    }
    auto boundNodeOffset =
        boundNodeIDVector.getValue<nodeID_t>(boundNodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(boundNodeOffset < StorageConstants::NODE_GROUP_SIZE);
    KU_ASSERT(updateState.columnID < updateChunksPerColumn.size());
    auto& relIDVector = updateState.relIDVector;
    auto relIDPos = relIDVector.state->selVector->selectedPositions[0];
    auto relOffset = relIDVector.getValue<relID_t>(relIDPos).offset;
    auto& rowIdxVector = updateState.localState.getRowIdxVector();
    // TODO: might still better to in place update insertChunks if newly inserted.
    updateInfos[updateState.columnID].offsetToRowIdx[relOffset] =
        rowIdxVector.getValue<offset_t>(rowIdxVector.state->selVector->selectedPositions[0]);
    updateInfos[updateState.columnID].srcNodeOffsetToRelOffsets[boundNodeOffset].push_back(
        relOffset);
    return true;
}

bool LocalRelNG::delete_(TableDeleteState& state) {
    auto& deleteState = ku_dynamic_cast<TableDeleteState&, RelTableDeleteState&>(state);
    auto& boundNodeIDVector = direction == RelDataDirection::FWD ? deleteState.srcNodeIDVector :
                                                                   deleteState.dstNodeIDVector;
    auto boundNodeIDPos = boundNodeIDVector.state->selVector->selectedPositions[0];
    if (boundNodeIDVector.isNull(boundNodeIDPos)) {
        return false;
    }
    auto boundNodeOffset =
        boundNodeIDVector.getValue<nodeID_t>(boundNodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(boundNodeOffset < StorageConstants::NODE_GROUP_SIZE);
    auto& relIDVector = deleteState.relIDVector;
    auto relIDPos = relIDVector.state->selVector->selectedPositions[0];
    auto relOffset = relIDVector.getValue<relID_t>(relIDPos).offset;
    if (deleteInfo.deletedOffsets.contains(relOffset)) {
        return false;
    }
    deleteInfo.deletedOffsets.insert(relOffset);
    deleteInfo.srcNodeOffsetToRelOffsetVec[boundNodeOffset].push_back(relOffset);
    return true;
}

offset_t LocalRelNG::getNumInsertedRels(offset_t srcOffset) const {
    if (insertInfo.srcNodeOffsetToRelOffsets.contains(srcOffset)) {
        return insertInfo.srcNodeOffsetToRelOffsets.at(srcOffset).size();
    }
    return 0;
}

void LocalRelNG::getChangesPerCSRSegment(
    std::vector<int64_t>& sizeChangesPerSegment, std::vector<bool>& hasChangesPerSegment) {
    auto numSegments = StorageConstants::NODE_GROUP_SIZE / StorageConstants::CSR_SEGMENT_SIZE;
    sizeChangesPerSegment.resize(numSegments, 0 /*initValue*/);
    hasChangesPerSegment.resize(numSegments, false /*initValue*/);
    for (auto& [srcOffset, insertions] : insertInfo.srcNodeOffsetToRelOffsets) {
        auto segmentIdx = getSegmentIdx(srcOffset);
        sizeChangesPerSegment[segmentIdx] += insertions.size();
        hasChangesPerSegment[segmentIdx] = true;
    }
    for (auto& [srcOffset, deletions] : deleteInfo.srcNodeOffsetToRelOffsetVec) {
        auto segmentIdx = getSegmentIdx(srcOffset);
        sizeChangesPerSegment[segmentIdx] -= deletions.size();
        hasChangesPerSegment[segmentIdx] = true;
    }
    for (auto& updateInfo : updateInfos) {
        for (auto& [srcOffset, _] : updateInfo.srcNodeOffsetToRelOffsets) {
            auto segmentIdx = getSegmentIdx(srcOffset);
            hasChangesPerSegment[segmentIdx] = true;
        }
    }
}

bool LocalRelNG::hasUpdatesOrInserts() const {
    return LocalNodeGroup::hasUpdatesOrInserts(updateInfos, insertInfo);
}

bool LocalRelNG::hasUpdatesOrDeletions() const {
    return LocalNodeGroup::hasUpdatesOrDeletions(updateInfos, deleteInfo);
}

void LocalRelNG::prepareCommit(transaction::Transaction* transaction,
    common::node_group_idx_t nodeGroupIdx, TableData& tableData) {
    auto& relTableData = ku_dynamic_cast<TableData&, RelTableData&>(tableData);
    relTableData.prepareCommitNodeGroup(transaction, nodeGroupIdx, this);
}

LocalRelTable::LocalRelTable(Table& table, WAL& wal) : LocalTable{table, wal} {
    std::vector<LogicalType> types;
    types.reserve(table.getNumColumns() + 1);
    types.push_back(*LogicalType::INTERNAL_ID());
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

void LocalRelTable::initializeReadState(TableReadState& state) {
    auto& dataReadState =
        ku_dynamic_cast<TableDataReadState&, RelDataReadState&>(*state.dataReadState);
    auto& localNodeGroups =
        dataReadState.direction == RelDataDirection::FWD ? fwdLocalNodeGroups : bwdLocalNodeGroups;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(dataReadState.startNodeOffset);
    if (localNodeGroups.contains(nodeGroupIdx)) {
        auto& localState = ku_dynamic_cast<LocalReadState&, LocalRelReadState&>(*state.localState);
        localState.localNodeGroup =
            ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(localNodeGroups.at(nodeGroupIdx).get());
    }
}

bool LocalRelTable::insert(TableInsertState& state) {
    auto& insertState = ku_dynamic_cast<TableInsertState&, RelTableInsertState&>(state);
    auto insertVectors =
        std::vector<ValueVector*>{const_cast<ValueVector*>(&insertState.srcNodeIDVector),
            const_cast<ValueVector*>(&insertState.dstNodeIDVector)};
    insertVectors.insert(insertVectors.end(), insertState.propertyVectors.begin(),
        insertState.propertyVectors.end());
    insertChunks->append(insertVectors, state.localState.getRowIdxVectorUnsafe());
    auto fwdInsertion = getLocalNodeGroup(
        RelDataDirection::FWD, insertState.srcNodeIDVector, NotExistAction::CREATE)
                            ->insert(state);
    auto bwdInsersion = getLocalNodeGroup(
        RelDataDirection::BWD, insertState.dstNodeIDVector, NotExistAction::CREATE)
                            ->insert(state);
    KU_ASSERT(fwdInsertion && bwdInsersion);
    return fwdInsertion && bwdInsersion;
}

bool LocalRelTable::update(TableUpdateState& state) {
    auto& updateState = ku_dynamic_cast<TableUpdateState&, RelTableUpdateState&>(state);
    KU_ASSERT(updateState.columnID < updateChunks.size());
    auto updateVectors =
        std::vector<ValueVector*>{const_cast<ValueVector*>(&updateState.propertyVector)};
    updateChunks[updateState.columnID]->append(
        updateVectors, state.localState.getRowIdxVectorUnsafe());
    auto fwdUpdated = getLocalNodeGroup(
        RelDataDirection::FWD, updateState.srcNodeIDVector, NotExistAction::CREATE)
                          ->update(updateState);
    auto bwdUpdated = getLocalNodeGroup(
        RelDataDirection::BWD, updateState.dstNodeIDVector, NotExistAction::CREATE)
                          ->update(updateState);
    KU_ASSERT(fwdUpdated == bwdUpdated);
    return fwdUpdated && bwdUpdated;
}

bool LocalRelTable::delete_(TableDeleteState& state) {
    auto& deleteState = ku_dynamic_cast<TableDeleteState&, RelTableDeleteState&>(state);
    auto fwdDeleted = getLocalNodeGroup(
        RelDataDirection::FWD, deleteState.srcNodeIDVector, NotExistAction::CREATE)
                          ->delete_(deleteState);
    auto bwdDeleted = getLocalNodeGroup(
        RelDataDirection::BWD, deleteState.dstNodeIDVector, NotExistAction::CREATE)
                          ->delete_(deleteState);
    KU_ASSERT(fwdDeleted == bwdDeleted);
    return fwdDeleted && bwdDeleted;
}

void LocalRelTable::scan(TableReadState&) {
    KU_UNREACHABLE;
}

void LocalRelTable::lookup(TableReadState&) {
    KU_UNREACHABLE;
}

LocalRelNG* LocalRelTable::getLocalNodeGroup(
    RelDataDirection direction, const ValueVector& nodeIDVector, NotExistAction action) {
    auto nodeIDPos = nodeIDVector.state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector.getValue<common::nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto& localNodeGroups =
        direction == RelDataDirection::FWD ? fwdLocalNodeGroups : bwdLocalNodeGroups;
    if (!localNodeGroups.contains(nodeGroupIdx)) {
        if (action == NotExistAction::CREATE) {
            localNodeGroups[nodeGroupIdx] = createLocalNodeGroup(direction, nodeGroupIdx);
        } else {
            return nullptr;
        }
    }
    KU_ASSERT(localNodeGroups.contains(nodeGroupIdx));
    return ku_dynamic_cast<LocalNodeGroup*, LocalRelNG*>(localNodeGroups.at(nodeGroupIdx).get());
}

std::unique_ptr<LocalNodeGroup> LocalRelTable::createLocalNodeGroup(
    RelDataDirection direction, node_group_idx_t nodeGroupIdx) {
    return std::make_unique<LocalRelNG>(insertChunks, updateChunks, direction,
        StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx));
}

void LocalRelTable::prepareCommit(transaction::Transaction* transaction) {
    auto& relTable = ku_dynamic_cast<Table&, RelTable&>(table);
    for (auto& [nodeGroupIdx, localNodeGroup] : fwdLocalNodeGroups) {
        localNodeGroup->prepareCommit(
            transaction, nodeGroupIdx, *relTable.getDirectedTableData(RelDataDirection::FWD));
    }
    for (auto& [nodeGroupIdx, localNodeGroup] : bwdLocalNodeGroups) {
        localNodeGroup->prepareCommit(
            transaction, nodeGroupIdx, *relTable.getDirectedTableData(RelDataDirection::BWD));
    }
    wal.addToUpdatedTables(table.getTableID());
}

void LocalRelTable::prepareRollback(transaction::Transaction*) {
    fwdLocalNodeGroups.clear();
    bwdLocalNodeGroups.clear();
}

} // namespace storage
} // namespace kuzu
