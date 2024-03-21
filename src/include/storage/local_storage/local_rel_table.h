#pragma once

#include "common/copy_constructors.h"
#include "common/enums/rel_direction.h"
#include "common/vector/value_vector.h"
#include "storage/local_storage/local_table.h"

namespace kuzu {
namespace storage {

// TODO: This should be updated.
static constexpr common::column_id_t LOCAL_NBR_ID_COLUMN_ID = 0;
static constexpr common::column_id_t LOCAL_REL_ID_COLUMN_ID = 1;

struct RelDataReadState;
class LocalRelNG final : public LocalNodeGroup {
public:
    LocalRelNG(std::shared_ptr<ChunkedNodeGroupCollection> insertChunks,
        std::vector<std::shared_ptr<ChunkedNodeGroupCollection>> updateChunks,
        common::RelDataDirection direction, common::offset_t nodeGroupStartOffset);
    DELETE_COPY_DEFAULT_MOVE(LocalRelNG);

    void scan(TableReadState& state) override;
    // For CSR, we need to apply updates and deletions here, while insertions are handled by
    // `scanCSR`.
    void applyLocalChangesToScannedVectors(common::offset_t srcOffset,
        const std::vector<common::column_id_t>& columnIDs, common::ValueVector* relIDVector,
        const std::vector<common::ValueVector*>& outputVectors);

    bool insert(TableInsertState& state) override;
    bool update(TableUpdateState& state) override;
    bool delete_(TableDeleteState& state) override;

    const LocalInsertUpdateInfo& getUpdateInfo(common::column_id_t columnID) const {
        return updateInfos[columnID];
    }
    const LocalInsertUpdateInfo& getInsertInfo() const { return insertInfo; }
    const LocalDeletionInfo& getDeleteInfo() const { return deleteInfo; }

    common::offset_t getNumInsertedRels(common::offset_t srcOffset) const;
    void getChangesPerCSRSegment(
        std::vector<int64_t>& sizeChangesPerSegment, std::vector<bool>& hasChangesPerSegment);

    void prepareCommit(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        TableData& tableData) override;

    bool hasUpdatesOrInserts() const override;
    bool hasUpdatesOrDeletions() const override;

private:
    static common::vector_idx_t getSegmentIdx(common::offset_t offset) {
        return offset >> common::StorageConstants::CSR_SEGMENT_SIZE_LOG2;
    }

    common::row_idx_t scanCSR(common::offset_t srcOffset, common::offset_t posToReadForOffset,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVector);
    void applyCSRUpdates(common::column_id_t columnID, common::ValueVector* relIDVector,
        common::ValueVector* outputVector);
    void applyCSRDeletions(common::offset_t srcOffsetInChunk, common::ValueVector* relIDVector);

private:
    common::RelDataDirection direction;
    LocalInsertUpdateInfo insertInfo;
    std::vector<LocalInsertUpdateInfo> updateInfos;
    LocalDeletionInfo deleteInfo;
};

class LocalRelTable final : public LocalTable {
public:
    LocalRelTable(Table& table, WAL& wal);

    void initializeReadState(TableReadState& state) override;
    void scan(TableReadState& state) override;
    void lookup(TableReadState& state) override;

    bool insert(TableInsertState& insertState) override;
    bool update(TableUpdateState& updateState) override;
    bool delete_(TableDeleteState& deleteState) override;

    void prepareCommit(transaction::Transaction* transaction) override;
    void prepareRollback(transaction::Transaction* transaction) override;

private:
    LocalRelNG* getLocalNodeGroup(common::RelDataDirection direction,
        const common::ValueVector& nodeIDVector,
        NotExistAction action = NotExistAction::RETURN_NULL);
    std::unique_ptr<LocalNodeGroup> createLocalNodeGroup(
        common::RelDataDirection direction, common::node_group_idx_t nodeGroupIdx);

private:
    std::unordered_map<common::node_group_idx_t, std::unique_ptr<LocalNodeGroup>>
        fwdLocalNodeGroups;
    std::unordered_map<common::node_group_idx_t, std::unique_ptr<LocalNodeGroup>>
        bwdLocalNodeGroups;
};

} // namespace storage
} // namespace kuzu
