#pragma once

#include "storage/store/node_group.h"

namespace kuzu {
namespace storage {

class NodeGroupCollection {
public:
    NodeGroupCollection() {}

    void append(const ChunkedNodeGroupCollection& chunkedGroupCollection);
    common::row_idx_t getNumRows() const;

private:
    std::vector<NodeGroup> nodeGroups;
};

} // namespace storage
} // namespace kuzu
