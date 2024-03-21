#include "storage/local_storage/local_table.h"

#include "storage/storage_utils.h"
#include "storage/store/table.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

LocalNodeGroup::LocalNodeGroup(std::shared_ptr<ChunkedNodeGroupCollection> insertChunks,
    std::vector<std::shared_ptr<ChunkedNodeGroupCollection>> updateChunks,
    offset_t nodeGroupStartOffset)
    : nodeGroupStartOffset{nodeGroupStartOffset}, insertChunks{insertChunks}, updateChunksPerColumn{
                                                                                  updateChunks} {}

bool LocalNodeGroup::hasUpdatesOrInserts(const std::vector<LocalInsertUpdateInfo>& updateInfos,
    const LocalInsertUpdateInfo& insertInfo) {
    if (!insertInfo.offsetToRowIdx.empty()) {
        return true;
    }
    for (auto& updateChunkInfo : updateInfos) {
        if (!updateChunkInfo.offsetToRowIdx.empty()) {
            return true;
        }
    }
    return false;
}

bool LocalNodeGroup::hasUpdatesOrDeletions(
    const std::vector<LocalInsertUpdateInfo>& updateInfos, const LocalDeletionInfo& deletionInfo) {
    if (!deletionInfo.deletedOffsets.empty()) {
        return true;
    }
    for (auto& updateChunkInfo : updateInfos) {
        if (!updateChunkInfo.offsetToRowIdx.empty()) {
            return true;
        }
    }
    return false;
}

} // namespace storage
} // namespace kuzu
