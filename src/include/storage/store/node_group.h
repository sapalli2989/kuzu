#pragma once

#include "storage/store/chunked_node_group.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

class Column;
struct TableScanState;
class NodeGroup {
public:
    NodeGroup() {}
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
