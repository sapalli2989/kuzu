#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class NodeGroup {
public:
    explicit NodeGroup(std::vector<std::unique_ptr<ColumnChunk>> chunks)
        : chunks{std::move(chunks)} {}

private:
    std::vector<std::unique_ptr<ColumnChunk>> chunks;
};

} // namespace storage
} // namespace kuzu
