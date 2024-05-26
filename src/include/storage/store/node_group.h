#pragma once

#include "storage/store/column_chunk.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

class Column;
struct TableScanState;
class NodeGroup {
public:
    explicit NodeGroup(std::vector<std::unique_ptr<ColumnChunk>> chunks)
        : chunks{std::move(chunks)} {}

    common::row_idx_t getNumRows() const;

    void scan(transaction::Transaction* transaction, const TableScanState& scanState,
        common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) const;
    void lookup(transaction::Transaction* transaction, TableScanState& scanState,
        common::ValueVector& nodeIDVector, const std::vector<common::ValueVector*>& outputVectors);

private:
    std::vector<std::unique_ptr<ColumnChunk>> chunks;
};

} // namespace storage
} // namespace kuzu
