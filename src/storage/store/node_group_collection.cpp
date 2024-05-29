#include "storage/store/node_group_collection.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void NodeGroupCollection::append(const ChunkedNodeGroupCollection& chunkedGroupCollection) {
    auto numRowsToAppend = chunkedGroupCollection.getNumRows();
    row_idx_t numRowsAppended = 0u;
    if (nodeGroups.empty()) {
        nodeGroups.push_back(NodeGroup{nodeGroups.size(), types});
    }
    while (numRowsAppended < numRowsToAppend) {
        if (nodeGroups.back().isFull()) {
            nodeGroups.push_back(NodeGroup{nodeGroups.size(), types});
        }
        auto& lastNodeGroup = nodeGroups.back();
        auto numToAppendInNodeGroup =
            std::min(numRowsToAppend - numRowsAppended, StorageConstants::NODE_GROUP_SIZE);
        lastNodeGroup.append(chunkedGroupCollection, numRowsAppended, numToAppendInNodeGroup);
        numRowsAppended += numToAppendInNodeGroup;
    }
}

row_idx_t NodeGroupCollection::getNumRows() const {
    row_idx_t numRows = 0;
    for (auto& nodeGroup : nodeGroups) {
        numRows += nodeGroup.getNumRows();
    }
    return numRows;
}

} // namespace storage
} // namespace kuzu
