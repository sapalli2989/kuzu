#pragma once

#include <string>
#include <vector>

#include "common/data_chunk/data_chunk.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "function/scalar_function.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"

namespace kuzu {
namespace processor {

class NpyReader {
public:
    explicit NpyReader(const std::string& filePath);

    ~NpyReader();

    size_t getNumElementsPerRow() const;

    uint8_t* getPointerToRow(size_t row) const;

    inline size_t getNumRows() const { return shape[0]; }

    void readBlock(common::block_idx_t blockIdx, common::ValueVector* vectorToRead) const;

    // Used in tests only.
    inline common::LogicalTypeID getType() const { return type; }
    inline std::vector<size_t> getShape() const { return shape; }

    void validate(const common::LogicalType& type_, common::offset_t numRows);

private:
    void parseHeader();
    void parseType(std::string descr);

private:
    std::string filePath;
    int fd;
    size_t fileSize;
    void* mmapRegion;
    size_t dataOffset;
    std::vector<size_t> shape;
    common::LogicalTypeID type;
    static inline const std::string defaultFieldName = "NPY_FIELD";
};

class NpyMultiFileReader {
public:
    explicit NpyMultiFileReader(const std::vector<std::string>& filePaths);

    void readBlock(common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead) const;

private:
    std::vector<std::unique_ptr<NpyReader>> fileReaders;
};

struct NpyScanBindInput final : public function::TableFuncBindInput {
    NpyScanBindInput(
        std::vector<std::unique_ptr<common::Value>> inputValues, const common::ReaderConfig config)
        : TableFuncBindInput{std::move(inputValues)}, config{config} {}
    const common::ReaderConfig config;
};

struct NpyScanSharedState final : public function::ScanSharedTableFuncState {
    explicit NpyScanSharedState(const common::ReaderConfig readerConfig)
        : ScanSharedTableFuncState{std::move(readerConfig)} {
        npyMultiFileReader = std::make_unique<NpyMultiFileReader>(this->readerConfig.filePaths);
    }

    std::unique_ptr<NpyMultiFileReader> npyMultiFileReader;
};

class NpyScanFunction {
public:
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        function::TableFuncBindInput* input, catalog::CatalogContent* catalog) {
        auto bindInput = reinterpret_cast<function::ScanTableFuncBindInput*>(input);
        (!config.filePaths.empty() && config.getNumFiles() == config.getNumColumns());
        row_idx_t numRows;
        for (auto i = 0u; i < config.getNumFiles(); i++) {
            auto reader = make_unique<NpyReader>(config.filePaths[i]);
            if (i == 0) {
                numRows = reader->getNumRows();
            }
            reader->validate(*config.columnTypes[i], numRows);
        }
        auto parquetScanBindInput = reinterpret_cast<NpyScanBindInput*>(input);
        return std::make_unique<function::ScanBindData>(
            common::LogicalType::copy(parquetScanBindInput->config.columnTypes),
            parquetScanBindInput->config.columnNames, parquetScanBindInput->config,
            parquetScanBindInput->mm);
    }

    static std::unique_ptr<function::SharedTableFuncState> initSharedState(
        function::TableFunctionInitInput& input) {
        auto bindData = reinterpret_cast<function::ScanBindData*>(input.bindData);
        return std::make_unique<NpyScanSharedState>(bindData->config);
    }

    static std::unique_ptr<function::LocalTableFuncState> initLocalState(
        function::TableFunctionInitInput& input, function::SharedTableFuncState* state) {
        return std::make_unique<function::LocalTableFuncState>();
    }
};

} // namespace processor
} // namespace kuzu
