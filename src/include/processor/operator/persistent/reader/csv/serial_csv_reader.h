#pragma once

#include "base_csv_reader.h"
#include "function/scalar_function.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"

namespace kuzu {
namespace processor {

//! Serial CSV reader is a class that reads values from a stream in a single thread.
class SerialCSVReader final : public BaseCSVReader {
public:
    SerialCSVReader(const std::string& filePath, const common::ReaderConfig& readerConfig);

    //! Sniffs CSV dialect and determines skip rows, header row, column types and column names
    std::vector<std::pair<std::string, common::LogicalType>> sniffCSV();
    uint64_t parseBlock(common::block_idx_t blockIdx, common::DataChunk& resultChunk) override;

protected:
    void handleQuotedNewline() override {}
};

class SerialCSVScanSharedState final : public function::ScanSharedTableFuncState {
public:
    explicit SerialCSVScanSharedState(const common::ReaderConfig readerConfig, uint64_t numRows)
        : ScanSharedTableFuncState{std::move(readerConfig), numRows} {
        reader = std::make_unique<SerialCSVReader>(readerConfig.filePaths[0], readerConfig);
    }

    void read(common::DataChunk& dataChunk) {
        std::lock_guard<std::mutex> mtx{lock};
        do {
            if (fileIdx > readerConfig.getNumFiles()) {
                return;
            }
            uint64_t numRows = reader->parseBlock(0 /* unused by serial csv reader */, dataChunk);
            dataChunk.state->selVector->selectedSize = numRows;
            if (numRows > 0) {
                return;
            }
            fileIdx++;
            if (fileIdx < readerConfig.getNumFiles()) {
                reader = std::make_unique<SerialCSVReader>(
                    readerConfig.filePaths[fileIdx], readerConfig);
            }
        } while (true);
    }

    std::unique_ptr<SerialCSVReader> reader;
};

class SerialCSVScan {
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
        return std::make_unique<SerialCSVScanSharedState>(bindData->config, numRows);
    }

    static std::unique_ptr<function::LocalTableFuncState> initLocalState(
        function::TableFunctionInitInput& /*input*/, function::SharedTableFuncState* /*state*/) {
        return std::make_unique<function::LocalTableFuncState>();
    }
};

} // namespace processor
} // namespace kuzu
