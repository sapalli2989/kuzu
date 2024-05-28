#include "storage/store/node_group.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

row_idx_t NodeGroup::getNumRows() const {
    row_idx_t numRows = 0;
    for (auto& chunkedGroup : chunkedGroups.getChunkedGroups()) {
        numRows += chunkedGroup->getNumRows();
    }
    return numRows;
}

} // namespace storage
} // namespace kuzu
