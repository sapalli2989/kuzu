#pragma once

#include "common/copy_constructors.h"
#include "local_table.h"

namespace kuzu {
namespace storage {

class LocalNodeNG final : public LocalNodeGroup {
public:
    LocalNodeNG(std::shared_ptr<ChunkedNodeGroupCollection> insertChunks,
        std::vector<std::shared_ptr<ChunkedNodeGroupCollection>> updateChunks,
        common::offset_t nodeGroupStartOffset)
        : LocalNodeGroup{insertChunks, updateChunks, nodeGroupStartOffset} {}
    DELETE_COPY_DEFAULT_MOVE(LocalNodeNG);

    void scan(TableReadState& state) override;

    bool insert(TableInsertState& state) override;
    bool update(TableUpdateState& state) override;
    bool delete_(TableDeleteState& state) override;

    bool hasUpdatesOrInserts() const override;
    bool hasUpdatesOrDeletions() const override;

    void prepareCommit(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        TableData& tableData) override;

    inline const offset_to_row_idx_t& getInsertInfoRef() { return insertInfo.offsetToRowIdx; }
    inline const offset_to_row_idx_t& getUpdateInfoRef(common::column_id_t columnID) {
        KU_ASSERT(columnID < updateInfos.size());
        return updateInfos[columnID].offsetToRowIdx;
    }

private:
    void lookupInternal(common::offset_t nodeOffset, common::column_id_t columnID,
        common::ValueVector& output, common::sel_t posInOutputVector);

private:
    LocalInsertUpdateInfo insertInfo;
    LocalDeletionInfo deleteInfo;
    std::vector<LocalInsertUpdateInfo> updateInfos;
};

// TODO(Guodong): Should move HashIndexLocalStorage here.
class LocalNodeTable final : public LocalTable {
public:
    LocalNodeTable(Table& table, WAL& wal);

    void initializeReadState(TableReadState&) override {
        // DO NOTHING.
    }
    void scan(TableReadState& state) override;
    void lookup(TableReadState& state) override;

    bool insert(TableInsertState& insertState) override;
    bool update(TableUpdateState& updateState) override;
    bool delete_(TableDeleteState& deleteState) override;

    void prepareCommit(transaction::Transaction* transaction) override;
    void prepareRollback(transaction::Transaction* transaction) override;

private:
    LocalNodeGroup* getLocalNodeGroup(const common::ValueVector& nodeIDVector,
        NotExistAction action = NotExistAction::RETURN_NULL);
    std::unique_ptr<LocalNodeGroup> createLocalNodeGroup(common::node_group_idx_t nodeGroupIdx);

private:
    std::unordered_map<common::node_group_idx_t, std::unique_ptr<LocalNodeGroup>> localNodeGroups;
};

} // namespace storage
} // namespace kuzu
