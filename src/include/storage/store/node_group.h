#pragma once

#include "storage/store/chunked_node_group_collection.h"

namespace kuzu {
namespace storage {

class NodeGroup {
public:
    explicit NodeGroup(common::node_group_idx_t nodeGroupIdx,
        const std::vector<common::LogicalType>& types)
        : nodeGroupIdx{nodeGroupIdx}, startNodeOffset{0}, chunkedGroups{types} {}

    common::row_idx_t getNumRows() const;

private:
    common::node_group_idx_t nodeGroupIdx;
    // Offset of the first node in the group.
    common::offset_t startNodeOffset;
    ChunkedNodeGroupCollection chunkedGroups;
};

} // namespace storage
} // namespace kuzu
