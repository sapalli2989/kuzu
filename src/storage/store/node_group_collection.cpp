#include "storage/store/node_group_collection.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void NodeGroupCollection::append(const ChunkedNodeGroupCollection& chunkedGroupCollection) {
    // TODO(Guodong): Implement this.
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
