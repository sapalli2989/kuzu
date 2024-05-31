#pragma once

#include "storage/store/chunked_node_group_collection.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

struct NodeGroupScanState {

    common::vector_idx_t chunkedGroupIdx = 0;
    common::row_idx_t nextRowToScan = 0;
    common::row_idx_t maxNumRowsToScan = 0;

    NodeGroupScanState() {}
    DELETE_COPY_DEFAULT_MOVE(NodeGroupScanState);

    bool hasNext() const { return nextRowToScan < maxNumRowsToScan; }
};

struct TableScanState;
class NodeGroup {
public:
    explicit NodeGroup(common::node_group_idx_t nodeGroupIdx,
        const std::vector<common::LogicalType>& dataTypes)
        : nodeGroupIdx{nodeGroupIdx}, dataTypes{dataTypes}, startNodeOffset{0},
          chunkedGroups{dataTypes} {}

    common::row_idx_t getNumRows() const { return chunkedGroups.getNumRows(); }
    bool isFull() const {
        return chunkedGroups.getNumRows() == common::StorageConstants::NODE_GROUP_SIZE;
    }
    const std::vector<common::LogicalType>& getDataTypes() const { return dataTypes; }
    void append(const ChunkedNodeGroupCollection& chunkCollection, common::row_idx_t offset,
        common::row_idx_t numRowsToAppend) {
        chunkedGroups.append(chunkCollection, offset, numRowsToAppend);
    }
    void append(transaction::Transaction* transaction, const ChunkedNodeGroup& chunkedGroup);
    void append(transaction::Transaction* transaction,
        std::unique_ptr<ChunkedNodeGroup> chunkedGroup);

    void initializeScanState(transaction::Transaction* transaction, TableScanState& state) const;
    void scan(transaction::Transaction* transaction, TableScanState& state) const;

    void flush(BMFileHandle& dataFH);

private:
    common::node_group_idx_t nodeGroupIdx;
    std::vector<common::LogicalType> dataTypes;
    // Offset of the first node in the group.
    common::offset_t startNodeOffset;
    ChunkedNodeGroupCollection chunkedGroups;
};

} // namespace storage
} // namespace kuzu
