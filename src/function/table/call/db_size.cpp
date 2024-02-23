#include "function/table/call_functions.h"
#include "storage/buffer_manager/buffer_manager.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct DBSizeBindData : public CallTableFuncBindData {
    uint64_t memUsage;

    DBSizeBindData(uint64_t memUsage, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          memUsage{memUsage} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DBSizeBindData>(memUsage, columnTypes, columnNames, maxOffset);
    }
};

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, CallFuncSharedState*>(input.sharedState);
    auto outputVector = dataChunk.getValueVector(0);
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        return 0;
    }
    auto bindData = ku_dynamic_cast<TableFuncBindData*, DBSizeBindData*>(input.bindData);
    auto pos = dataChunk.state->selVector->selectedPositions[0];
    outputVector->setValue<uint64_t>(pos, bindData->memUsage);
    outputVector->setNull(pos, false);
    return 1;
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context, TableFuncBindInput*) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("mem_usage");
    returnTypes.emplace_back(*LogicalType::UINT64());
    auto memUsage = context->getMemoryManager()->getBufferManager()->getMemoryUsage();
    return std::make_unique<DBSizeBindData>(
        memUsage, std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
}

function_set DBSizeFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(DB_SIZE_FUNC_NAME, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
