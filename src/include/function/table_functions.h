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

    ScanSharedTableFuncState(const common::ReaderConfig readerConfig)
        : fileIdx{0}, blockIdx{0}, readerConfig{std::move(readerConfig)} {}
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

struct TableFunction : public Function {
    table_func_t tableFunc;
    table_func_bind_t bindFunc;
    table_func_init_shared_t initSharedStateFunc;
    table_func_init_local_t initLocalStateFunc;

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
