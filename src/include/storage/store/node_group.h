#pragma once

#include "storage/store/chunked_node_group_collection.h"

namespace kuzu {
namespace storage {

class NodeGroup {
public:
    explicit NodeGroup(common::node_group_idx_t nodeGroupIdx,
        const std::vector<common::LogicalType>& types)
        : nodeGroupIdx{nodeGroupIdx}, types{types}, startNodeOffset{0}, chunkedGroups{types} {}

    common::row_idx_t getNumRows() const { return chunkedGroups.getNumRows(); }
    bool isFull() const {
        return chunkedGroups.getNumRows() == common::StorageConstants::NODE_GROUP_SIZE;
    }
    const std::vector<common::LogicalType>& getDataTypes() const { return types; }
    void append(const ChunkedNodeGroupCollection& chunkCollection, common::row_idx_t offset,
        common::row_idx_t numRowsToAppend) {
        chunkedGroups.append(chunkCollection, offset, numRowsToAppend);
    }

private:
    common::node_group_idx_t nodeGroupIdx;
    std::vector<common::LogicalType> types;
    // Offset of the first node in the group.
    common::offset_t startNodeOffset;
    ChunkedNodeGroupCollection chunkedGroups;
};

} // namespace storage
} // namespace kuzu
