#pragma once

#include "base_csv_reader.h"
#include "common/types/types.h"
#include "function/scalar_function.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"
#include "serial_csv_reader.h"

namespace kuzu {
namespace processor {

//! ParallelCSVReader is a class that reads values from a stream in parallel.
class ParallelCSVReader final : public BaseCSVReader {
    friend class ParallelParsingDriver;

public:
    ParallelCSVReader(const std::string& filePath, const common::ReaderConfig& readerConfig);

    bool hasMoreToRead() const;
    uint64_t parseBlock(common::block_idx_t blockIdx, common::DataChunk& resultChunk) override;
    uint64_t continueBlock(common::DataChunk& resultChunk);

protected:
    void handleQuotedNewline() override;

private:
    bool finishedBlock() const;
    void seekToBlockStart();
};

struct ParallelCSVLocalState final : public function::LocalTableFuncState {
    std::unique_ptr<ParallelCSVReader> reader;
    uint64_t fileIdx;
};

class ParallelCSVScanSharedState final : public function::ScanSharedTableFuncState {
public:
    explicit ParallelCSVScanSharedState(const common::ReaderConfig readerConfig,
        storage::MemoryManager* memoryManager, uint64_t numRows)
        : ScanSharedTableFuncState{std::move(readerConfig), numRows}, memoryManager{memoryManager} {
    }

    storage::MemoryManager* memoryManager;
};

class ParallelCSVScan {
public:
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        function::TableFuncBindInput* input, catalog::CatalogContent* catalog) {
        auto csvScanBindInput = reinterpret_cast<function::ScanTableFuncBindInput*>(input);
        return std::make_unique<function::ScanBindData>(
            common::LogicalType::copy(csvScanBindInput->config.columnTypes),
            csvScanBindInput->config.columnNames, csvScanBindInput->config, csvScanBindInput->mm);
    }

    static std::unique_ptr<function::SharedTableFuncState> initSharedState(
        function::TableFunctionInitInput& input) {
        auto bindData = reinterpret_cast<function::ScanBindData*>(input.bindData);
        common::row_idx_t numRows = 0;
        for (const auto& path : bindData->config.filePaths) {
            auto reader = make_unique<SerialCSVReader>(path, bindData->config);
            numRows += reader->countRows();
        }
        return std::make_unique<ParallelCSVScanSharedState>(
            bindData->config, bindData->mm, numRows);
    }

    static std::unique_ptr<function::LocalTableFuncState> initLocalState(
        function::TableFunctionInitInput& /*input*/, function::SharedTableFuncState* state) {
        auto localState = std::make_unique<ParallelCSVLocalState>();
        auto scanSharedState = reinterpret_cast<function::ScanSharedTableFuncState*>(state);
        localState->reader = std::make_unique<ParallelCSVReader>(
            scanSharedState->readerConfig.filePaths[0], scanSharedState->readerConfig);
        localState->fileIdx = 0;
        return localState;
    }
};

} // namespace processor
} // namespace kuzu
