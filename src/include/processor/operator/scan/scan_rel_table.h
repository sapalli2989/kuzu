#pragma once

#include "processor/operator/scan/scan_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

struct ScanRelTableInfo {
    storage::RelTable* table;
    common::RelDataDirection direction;
    std::vector<common::column_id_t> columnIDs;

    ScanRelTableInfo(storage::RelTable* table, common::RelDataDirection direction,
        std::vector<common::column_id_t> columnIDs)
        : table{table}, direction{direction}, columnIDs{std::move(columnIDs)} {}
    ScanRelTableInfo(const ScanRelTableInfo& other)
        : table{other.table}, direction{other.direction}, columnIDs{other.columnIDs} {}
    EXPLICIT_COPY_DEFAULT_MOVE(ScanRelTableInfo);
};

class ScanRelTable : public ScanTable {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::SCAN_REL_TABLE;

public:
    ScanRelTable(ScanTableInfo info, ScanRelTableInfo relInfo,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : ScanTable{type_, std::move(info), std::move(child), id, paramsString},
          relInfo{std::move(relInfo)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanRelTable>(info.copy(), relInfo.copy(), children[0]->clone(), id,
            paramsString);
    }

protected:
    ScanRelTableInfo relInfo;
    std::unique_ptr<storage::RelTableScanState> readState;
};

} // namespace processor
} // namespace kuzu
