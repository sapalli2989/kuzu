#pragma once

#include "common/copier_config/rdf_config.h"
#include "common/data_chunk/data_chunk.h"
#include "function/scalar_function.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"
#include "serd.h"

namespace kuzu {
namespace processor {

class RDFReader {
public:
    explicit RDFReader(std::string filePath, std::unique_ptr<common::RdfReaderConfig> config);

    ~RDFReader();

    common::offset_t read(common::DataChunk* dataChunkToRead);
    common::offset_t countLine();

private:
    static SerdStatus errorHandle(void* handle, const SerdError* error);
    static SerdStatus readerStatementSink(void* handle, SerdStatementFlags flags,
        const SerdNode* graph, const SerdNode* subject, const SerdNode* predicate,
        const SerdNode* object, const SerdNode* object_datatype, const SerdNode* object_lang);
    static SerdStatus prefixSink(void* handle, const SerdNode* name, const SerdNode* uri);

    static SerdStatus counterStatementSink(void* handle, SerdStatementFlags flags,
        const SerdNode* graph, const SerdNode* subject, const SerdNode* predicate,
        const SerdNode* object, const SerdNode* object_datatype, const SerdNode* object_lang);

private:
    const std::string filePath;
    std::unique_ptr<common::RdfReaderConfig> config;

    FILE* fp;
    SerdReader* reader;
    SerdReader* counter;

    // TODO(Xiyang): use prefix to expand CURIE.
    const char* currentPrefix;
    common::offset_t rowOffset;
    common::offset_t vectorSize;
    SerdStatus status;

    common::ValueVector* sVector; // subject
    common::ValueVector* pVector; // predicate
    common::ValueVector* oVector; // object

    std::unique_ptr<common::ValueVector> sOffsetVector;
    std::unique_ptr<common::ValueVector> pOffsetVector;
    std::unique_ptr<common::ValueVector> oOffsetVector;
};

struct RDFScanLocalState final : public function::LocalTableFuncState {

    std::unique_ptr<RDFReader> reader;
};

class RDFScan {
public:
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        function::TableFuncBindInput* input, catalog::CatalogContent* catalog) {
        auto rdfScanBindData = reinterpret_cast<function::ScanTableFuncBindInput*>(input);
        return std::make_unique<function::ScanBindData>(
            common::LogicalType::copy(rdfScanBindData->config.columnTypes),
            rdfScanBindData->config.columnNames, rdfScanBindData->config, rdfScanBindData->mm);
    }

    static std::unique_ptr<function::SharedTableFuncState> initSharedState(
        function::TableFunctionInitInput& input) {
        auto bindData = reinterpret_cast<function::ScanBindData*>(input.bindData);
        auto reader = make_unique<RDFReader>(
            bindData->config.filePaths[0], bindData->config.rdfReaderConfig->copy());
        return std::make_unique<function::ScanSharedTableFuncState>(
            bindData->config, reader->countLine());
    }

    static std::unique_ptr<function::LocalTableFuncState> initLocalState(
        function::TableFunctionInitInput& input, function::SharedTableFuncState* /*state*/) {
        auto bindData = reinterpret_cast<function::ScanBindData*>(input.bindData);
        auto localState = std::make_unique<RDFScanLocalState>();
        localState->reader = std::make_unique<RDFReader>(
            bindData->config.filePaths[0], bindData->config.rdfReaderConfig->copy());
        return localState;
    }
};

} // namespace processor
} // namespace kuzu
