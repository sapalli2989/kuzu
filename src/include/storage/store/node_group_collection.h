#pragma once

#include <shared_mutex>

#include "storage/store/node_group.h"

namespace kuzu {
namespace storage {

class NodeGroupCollection {
public:
    NodeGroupCollection() = default;
    explicit NodeGroupCollection(const std::vector<common::LogicalType>& types) : types{types} {}

    void append(const ChunkedNodeGroupCollection& chunkedGroupCollection);
    common::row_idx_t getNumRows();
    common::node_group_idx_t getNumNodeGroups() {
        std::shared_lock sLck{mtx};
        return nodeGroups.size();
    }
    const NodeGroup& getNodeGroup(common::node_group_idx_t groupIdx) {
        std::shared_lock sLck{mtx};
        KU_ASSERT(groupIdx < nodeGroups.size());
        return *nodeGroups[groupIdx];
    }
    void setNodeGroup(common::node_group_idx_t nodeGroupIdx, std::unique_ptr<NodeGroup> group) {
        std::unique_lock xLck{mtx};
        nodeGroups.resize(nodeGroupIdx + 1);
        nodeGroups[nodeGroupIdx] = std::move(group);
    }

    // TODO(Guodong): Rename to `mergeAndFlushWhenFull`.
    void merge(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        const ChunkedNodeGroup& chunkedGroup);

private:
    std::shared_mutex mtx;
    std::vector<common::LogicalType> types;
    std::vector<std::unique_ptr<NodeGroup>> nodeGroups;
    BMFileHandle* dataFH = nullptr;
};

} // namespace storage
} // namespace kuzu
