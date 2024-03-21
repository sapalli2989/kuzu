#pragma once

#include <unordered_map>

#include "common/vector/value_vector.h"
#include "storage/store/chunked_node_group_collection.h"

namespace kuzu {
namespace transaction {
class Transaction;
}
namespace storage {

using offset_to_row_idx_t = std::unordered_map<common::offset_t, common::row_idx_t>;
using offset_to_row_idx_vec_t =
    std::unordered_map<common::offset_t, std::vector<common::row_idx_t>>;
using offset_set_t = std::unordered_set<common::offset_t>;

static constexpr common::column_id_t NBR_ID_COLUMN_ID = 0;
static constexpr common::column_id_t REL_ID_COLUMN_ID = 1;

struct LocalInsertUpdateInfo {
    offset_to_row_idx_t offsetToRowIdx;
    // Only used for rel tables. Should be moved out later.
    // TODO: Should just map from srcNodeOffset to rowIdx.
    offset_to_row_idx_vec_t srcNodeOffsetToRelOffsets;
};

struct LocalDeletionInfo {
    // The offset here can either be nodeOffset ( for node table) or relOffset (for rel table).
    offset_set_t deletedOffsets;
    // Only used for rel tables. Should be moved out later.
    offset_to_row_idx_vec_t srcNodeOffsetToRelOffsetVec;
};

struct TableInsertState;
struct TableUpdateState;
struct TableDeleteState;
struct TableReadState;

class Table;
class TableData;
class LocalNodeGroup {
public:
    LocalNodeGroup(std::shared_ptr<ChunkedNodeGroupCollection> insertChunks,
        std::vector<std::shared_ptr<ChunkedNodeGroupCollection>> updateChunks,
        common::offset_t nodeGroupStartOffset);
    virtual ~LocalNodeGroup() = default;

    virtual void scan(TableReadState& state) = 0;

    virtual bool insert(TableInsertState& state) = 0;
    virtual bool update(TableUpdateState& state) = 0;
    virtual bool delete_(TableDeleteState& state) = 0;

    const ChunkedNodeGroupCollection& getUpdateChunks(common::column_id_t columnID) {
        KU_ASSERT(columnID < updateChunksPerColumn.size());
        return *updateChunksPerColumn[columnID];
    }
    const ChunkedNodeGroupCollection& getInsertChunks() const { return *insertChunks; }

    virtual bool hasUpdatesOrInserts() const = 0;
    virtual bool hasUpdatesOrDeletions() const = 0;

    virtual void prepareCommit(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, TableData& tableData) = 0;

protected:
    static bool hasUpdatesOrInserts(const std::vector<LocalInsertUpdateInfo>& updateInfos,
        const LocalInsertUpdateInfo& insertInfo);
    static bool hasUpdatesOrDeletions(const std::vector<LocalInsertUpdateInfo>& updateInfos,
        const LocalDeletionInfo& deletionInfo);

protected:
    common::offset_t nodeGroupStartOffset;
    std::shared_ptr<ChunkedNodeGroupCollection> insertChunks;
    std::vector<std::shared_ptr<ChunkedNodeGroupCollection>> updateChunksPerColumn;
};

enum class NotExistAction { CREATE, RETURN_NULL };

class WAL;
class LocalTable {
public:
    virtual ~LocalTable() = default;

    virtual void initializeReadState(TableReadState& state) = 0;
    virtual void scan(TableReadState& state) = 0;
    virtual void lookup(TableReadState& state) = 0;

    virtual bool insert(TableInsertState& insertState) = 0;
    virtual bool update(TableUpdateState& updateState) = 0;
    virtual bool delete_(TableDeleteState& deleteState) = 0;

    virtual void prepareCommit(transaction::Transaction* transaction) = 0;
    virtual void prepareRollback(transaction::Transaction* transaction) = 0;

protected:
    explicit LocalTable(Table& table, WAL& wal) : table{table}, wal{wal} {}

protected:
    Table& table;
    WAL& wal;
    std::shared_ptr<ChunkedNodeGroupCollection> insertChunks;
    std::vector<std::shared_ptr<ChunkedNodeGroupCollection>> updateChunks;
};

} // namespace storage
} // namespace kuzu
