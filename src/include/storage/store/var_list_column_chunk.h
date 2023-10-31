#pragma once

#include "storage/store/column_chunk.h"
#include <bit>

namespace kuzu {
namespace storage {

class VarListColumnChunk : public ColumnChunk {
public:
    VarListColumnChunk(common::LogicalType dataType, bool enableCompression);

    inline ColumnChunk* getDataColumnChunk() const { return varListDataColumnChunk.get(); }

    void resetToEmpty() final;

    void append(common::ValueVector* vector, common::offset_t startPosInChunk) final;

    inline void resizeDataChunkForValues(uint64_t numValues) {
        if (numValues > varListDataColumnChunk->getCapacity()) {
            varListDataColumnChunk->resize(std::bit_ceil(numValues));
        }
    }

    inline void resizeDataColumnChunk(uint64_t numBytesForBuffer) {
        // TODO(bmwinger): This won't work properly for booleans (will be one eighth as many values
        // as could fit)
        auto numValues = varListDataColumnChunk->getNumBytesPerValue() == 0 ?
                             0 :
                             numBytesForBuffer / varListDataColumnChunk->getNumBytesPerValue();
        resizeDataChunkForValues(numValues);
    }

private:
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;

    void write(const common::Value& listVal, uint64_t posToWrite) override;

    void copyListValues(const common::list_entry_t& entry, common::ValueVector* dataVector);

    inline uint64_t getListLen(common::offset_t offset) const {
        return getListOffset(offset + 1) - getListOffset(offset);
    }

    inline common::offset_t getListOffset(common::offset_t offset) const {
        return offset == 0 ? 0 : getValue<uint64_t>(offset - 1);
    }

private:
    std::unique_ptr<ColumnChunk> varListDataColumnChunk;
};

} // namespace storage
} // namespace kuzu
