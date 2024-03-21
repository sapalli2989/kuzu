#pragma once

#include "storage/store/chunked_node_group.h"

namespace kuzu {
namespace storage {

class ChunkedNodeGroupCollection {
public:
    static constexpr uint64_t CHUNK_CAPACITY = 2048;

    explicit ChunkedNodeGroupCollection(std::vector<common::LogicalType> types)
        : types{std::move(types)}, numRows{0} {}
    DELETE_COPY_DEFAULT_MOVE(ChunkedNodeGroupCollection);

    static std::pair<uint64_t, common::offset_t> getChunkIdxAndOffsetInChunk(
        common::row_idx_t rowIdx) {
        return std::make_pair(rowIdx / CHUNK_CAPACITY, rowIdx % CHUNK_CAPACITY);
    }

    inline const std::vector<std::unique_ptr<ChunkedNodeGroup>>& getChunkedGroups() const {
        return chunkedGroups;
    }
    inline const ChunkedNodeGroup* getChunkedGroup(common::node_group_idx_t groupIdx) const {
        KU_ASSERT(groupIdx < chunkedGroups.size());
        return chunkedGroups[groupIdx].get();
    }
    inline ChunkedNodeGroup* getChunkedGroupUnsafe(common::node_group_idx_t groupIdx) {
        KU_ASSERT(groupIdx < chunkedGroups.size());
        return chunkedGroups[groupIdx].get();
    }

    void append(
        const std::vector<common::ValueVector*>& vectors, const common::SelectionVector& selVector);
    void append(const ColumnChunk& offsetChunk, const ChunkedNodeGroup& chunkedGroup);

    void merge(std::unique_ptr<ChunkedNodeGroup> chunkedGroup);
    void merge(ChunkedNodeGroupCollection& other);

    inline uint64_t getNumChunkedGroups() const { return chunkedGroups.size(); }
    inline void clear() { chunkedGroups.clear(); }
    // `rowIdxVector` contains rowIdxes for each row appended from `dataVectorsToAppend`.
    void append(const std::vector<common::ValueVector*>& dataVectorsToAppend,
        common::ValueVector& rowIdxVector);

    ChunkCollection getChunkCollection(common::column_id_t columnID) const {
        ChunkCollection chunkCollection;
        for (auto& chunkedGroup : chunkedGroups) {
            chunkCollection.push_back(&chunkedGroup->getColumnChunk(columnID));
        }
        return chunkCollection;
    }

private:
    std::vector<common::LogicalType> types;
    std::vector<std::unique_ptr<ChunkedNodeGroup>> chunkedGroups;
    common::row_idx_t numRows;
};

} // namespace storage
} // namespace kuzu
