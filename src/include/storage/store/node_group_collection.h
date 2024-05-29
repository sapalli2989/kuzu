#pragma once

#include "storage/store/node_group.h"

namespace kuzu {
namespace storage {

class NodeGroupCollection {
public:
    NodeGroupCollection() = default;
    explicit NodeGroupCollection(const std::vector<common::LogicalType>& types) : types{types} {}
    DELETE_COPY_DEFAULT_MOVE(NodeGroupCollection);

    void append(const ChunkedNodeGroupCollection& chunkedGroupCollection);
    common::row_idx_t getNumRows() const;

private:
    std::vector<common::LogicalType> types;
    std::vector<NodeGroup> nodeGroups;
};

} // namespace storage
} // namespace kuzu
