#pragma once

#include "storage/stats/table_statistics_collection.h"
#include "storage/store/table_data.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace storage {

struct LocalReadState {
    virtual ~LocalReadState() = default;
    DELETE_COPY_DEFAULT_MOVE(LocalReadState);
};

struct TableReadState {
    const common::ValueVector& nodeIDVector;
    std::vector<common::column_id_t> columnIDs;
    const std::vector<common::ValueVector*>& outputVectors;
    std::unique_ptr<TableDataReadState> dataReadState;
    // Following fields are used for reading from transaction local storage.
    bool readFromLocalStorage;
    std::unique_ptr<LocalReadState> localState;

    TableReadState(const common::ValueVector& nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors)
        : nodeIDVector{nodeIDVector}, columnIDs{std::move(columnIDs)}, outputVectors{
                                                                           outputVectors} {
        dataReadState = std::make_unique<TableDataReadState>();
    }
    virtual ~TableReadState() = default;
};

struct LocalWriteState {
    explicit LocalWriteState() : rowIdxVector{*common::LogicalType::INT64()} {
        rowIdxVector.state = std::make_unique<common::DataChunkState>();
    }

    common::ValueVector& getRowIdxVectorUnsafe() { return rowIdxVector; }
    const common::ValueVector& getRowIdxVector() const { return rowIdxVector; }

private:
    common::ValueVector rowIdxVector;
};

struct TableInsertState {
    const std::vector<common::ValueVector*>& propertyVectors;
    LocalWriteState localState;

    explicit TableInsertState(const std::vector<common::ValueVector*>& propertyVectors)
        : propertyVectors{propertyVectors} {}
    virtual ~TableInsertState() = default;
};

struct TableUpdateState {
    common::column_id_t columnID;
    const common::ValueVector& propertyVector;
    LocalWriteState localState;

    TableUpdateState(common::column_id_t columnID, const common::ValueVector& propertyVector)
        : columnID{columnID}, propertyVector{propertyVector} {}
    virtual ~TableUpdateState() = default;
};

struct TableDeleteState {
    virtual ~TableDeleteState() = default;

    LocalWriteState localState;
};

class LocalTable;
class Table {
public:
    Table(catalog::TableCatalogEntry* tableEntry, TablesStatistics* tablesStatistics,
        MemoryManager* memoryManager, WAL* wal)
        : tableType{tableEntry->getTableType()}, tableID{tableEntry->getTableID()},
          tableName{tableEntry->getName()}, tablesStatistics{tablesStatistics},
          memoryManager{memoryManager}, bufferManager{memoryManager->getBufferManager()}, wal{wal} {
    }
    virtual ~Table() = default;

    inline common::TableType getTableType() const { return tableType; }
    inline common::table_id_t getTableID() const { return tableID; }
    inline common::row_idx_t getNumTuples(transaction::Transaction* transaction) const {
        return tablesStatistics->getNumTuplesForTable(transaction, tableID);
    }
    inline void updateNumTuplesByValue(uint64_t numTuples) {
        tablesStatistics->updateNumTuplesByValue(tableID, numTuples);
    }

    virtual void read(transaction::Transaction* transaction, TableReadState& readState) = 0;

    virtual void insert(transaction::Transaction* transaction, TableInsertState& insertState) = 0;
    virtual void update(transaction::Transaction* transaction, TableUpdateState& updateState) = 0;
    virtual void delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) = 0;

    virtual void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        common::ValueVector* defaultValueVector) = 0;
    virtual void dropColumn(common::column_id_t columnID) = 0;

    virtual common::column_id_t getNumColumns() const = 0;
    virtual common::LogicalType getColumnType(common::column_id_t columnID) const = 0;

    virtual void prepareCommit(transaction::Transaction* transaction, LocalTable* localTable) = 0;
    virtual void prepareRollback(LocalTable* localTable) = 0;
    virtual void checkpointInMemory() = 0;
    virtual void rollbackInMemory() = 0;

protected:
    common::TableType tableType;
    common::table_id_t tableID;
    std::string tableName;
    TablesStatistics* tablesStatistics;
    MemoryManager* memoryManager;
    BufferManager* bufferManager;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
