#pragma once

#include "common/data_chunk/data_chunk.h"
#include "function.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace catalog {
class CatalogContent;
} // namespace catalog
namespace common {
class ValueVector;
}
namespace main {
class ClientContext;
}

namespace function {

struct TableFuncBindData;
struct TableFuncBindInput;

struct SharedTableFuncState {
    virtual ~SharedTableFuncState() = default;
};

struct ScanSharedTableFuncState : public SharedTableFuncState {
    std::mutex lock;
    uint64_t fileIdx;
    uint64_t blockIdx;
    const common::ReaderConfig readerConfig;
    uint64_t numRows;

    ScanSharedTableFuncState(const common::ReaderConfig readerConfig, uint64_t numRows)
        : fileIdx{0}, blockIdx{0}, readerConfig{std::move(readerConfig)}, numRows{numRows} {}

    std::pair<uint64_t, uint64_t> getNext() {
        std::lock_guard<std::mutex> guard{lock};
        if (fileIdx >= readerConfig.getNumFiles()) {
            return {UINT64_MAX, UINT64_MAX};
        }
        return {fileIdx, blockIdx++};
    }

    void moveToNextFile(uint64_t completedFileIdx) {
        std::lock_guard<std::mutex> guard{lock};
        if (completedFileIdx == fileIdx) {
            blockIdx = 0;
            fileIdx++;
        }
    }
};

struct LocalTableFuncState {
    virtual ~LocalTableFuncState() = default;
};

struct TableFunctionInput {
    TableFuncBindData* bindData;
    LocalTableFuncState* localState;
    SharedTableFuncState* sharedState;

    TableFunctionInput(TableFuncBindData* bindData, LocalTableFuncState* localState,
        SharedTableFuncState* sharedState)
        : bindData{bindData}, localState{localState}, sharedState{sharedState} {}
};

struct TableFunctionInitInput {
    TableFuncBindData* bindData;

    TableFunctionInitInput(TableFuncBindData* bindData) : bindData{bindData} {}

    virtual ~TableFunctionInitInput() = default;
};

typedef std::unique_ptr<TableFuncBindData> (*table_func_bind_t)(main::ClientContext* /*context*/,
    TableFuncBindInput* /*input*/, catalog::CatalogContent* /*catalog*/);
typedef void (*table_func_t)(TableFunctionInput& data, common::DataChunk& output);
typedef std::unique_ptr<SharedTableFuncState> (*table_func_init_shared_t)(
    TableFunctionInitInput& input);
typedef std::unique_ptr<LocalTableFuncState> (*table_func_init_local_t)(
    TableFunctionInitInput& input, SharedTableFuncState* state);
typedef bool (*table_func_can_parallel_t)();

struct TableFunction : public Function {
    table_func_t tableFunc;
    table_func_bind_t bindFunc;
    table_func_init_shared_t initSharedStateFunc;
    table_func_init_local_t initLocalStateFunc;
    table_func_can_parallel_t canParallelFunc = [] { return true; };

    TableFunction(std::string name, table_func_t tableFunc, table_func_bind_t bindFunc,
        table_func_init_shared_t initSharedFunc, table_func_init_local_t initLocalFunc,
        std::vector<common::LogicalTypeID> inputTypes)
        : Function{FunctionType::TABLE, std::move(name), std::move(inputTypes)},
          tableFunc{std::move(tableFunc)}, bindFunc{std::move(bindFunc)},
          initSharedStateFunc{initSharedFunc}, initLocalStateFunc{initLocalFunc} {}

    inline std::string signatureToString() const override {
        return common::LogicalTypeUtils::dataTypesToString(parameterTypeIDs);
    }
};

} // namespace function
} // namespace kuzu
