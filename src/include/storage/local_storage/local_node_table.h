#pragma once

#include "common/copy_constructors.h"
#include "storage/local_storage/hash_index_local_storage.h"
#include "storage/local_storage/local_table.h"

namespace kuzu {
namespace storage {

class ChunkedNodeGroup;
struct TableScanState;

// class LocalNodeNG final : public LocalNodeGroup {
// public:
//     LocalNodeNG(common::table_id_t tableID, common::offset_t nodeGroupStartOffset,
//         const std::vector<common::LogicalType>& dataTypes)
//         : LocalNodeGroup{nodeGroupStartOffset, dataTypes}, tableID{tableID} {}
//     DELETE_COPY_DEFAULT_MOVE(LocalNodeNG);
//
//     void initializeScanState(TableScanState& scanState) const;
//     void scan(TableScanState& scanState);
//     void lookup(const common::ValueVector& nodeIDVector,
//         const std::vector<common::column_id_t>& columnIDs,
//         const std::vector<common::ValueVector*>& outputVectors);
//
//     bool insert(std::vector<common::ValueVector*> nodeIDVectors,
//         std::vector<common::ValueVector*> propertyVectors) override;
//     bool update(std::vector<common::ValueVector*> nodeIDVectors, common::column_id_t columnID,
//         common::ValueVector* propertyVector) override;
//     bool delete_(common::ValueVector* nodeIDVector,
//         common::ValueVector* /*extraVector*/ = nullptr) override;
//
//     const offset_to_row_idx_t& getInsertInfoRef() const { return
//     insertChunks.getOffsetToRowIdx(); } const offset_to_row_idx_t&
//     getUpdateInfoRef(common::column_id_t columnID) const {
//         return getUpdateChunks(columnID).getOffsetToRowIdx();
//     }
//
// private:
//     common::table_id_t tableID;
// };

// class LocalNodeTableData final : public LocalTableData {
// public:
//     explicit LocalNodeTableData(common::table_id_t tableID,
//         std::vector<common::LogicalType> dataTypes)
//         : LocalTableData{tableID, std::move(dataTypes)} {}
//
// private:
//     LocalNodeGroup* getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector) override;
// };

struct TableReadState;
class LocalNodeTable final : public LocalTable {
public:
    explicit LocalNodeTable(Table& table);
    DELETE_COPY_DEFAULT_MOVE(LocalNodeTable);

    bool insert(TableInsertState& insertState) override;
    bool update(TableUpdateState& updateState) override;
    bool delete_(TableDeleteState& deleteState) override;

    void initializeScanState(TableScanState& scanState) const;
    void scan(TableScanState& scanState) const;

    common::row_idx_t getNumRows() const { return chunkedGroups.getNumRows(); }

    const ChunkedNodeGroupCollection& getChunkedGroups() const { return chunkedGroups; }

private:
    std::unique_ptr<OverflowFile> overflowFile;
    std::unique_ptr<OverflowFileHandle> overflowFileHandle;
    std::unique_ptr<LocalHashIndex> hashIndex;
    ChunkedNodeGroupCollection chunkedGroups;
    // TODO: 1. Maintain a chunked group collection here for all inserted tuples. Each chunked group
    // is of size 2048, same size for the basic unit of scan (a vector at a time).
    //       - We do not keep index mapping from node offset to row idx.
    //       - We do not materialize node offsets, but rely on a virtual node offset. 2^63 is the
    //       base offset.
    // TODO: 2. Maintain a local hash index here for newly inserted tuples.
};

} // namespace storage
} // namespace kuzu
