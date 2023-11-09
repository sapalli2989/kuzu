#include "processor/operator/persistent/reader/csv/parallel_csv_reader.h"

#if defined(_WIN32)
#include <io.h>
#else
#include <unistd.h>
#endif

#include "common/exception/copy.h"
#include "common/string_format.h"
#include "common/system_message.h"
#include "processor/operator/persistent/reader/csv/driver.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

ParallelCSVReader::ParallelCSVReader(
    const std::string& filePath, const common::ReaderConfig& readerConfig)
    : BaseCSVReader{filePath, readerConfig} {}

bool ParallelCSVReader::hasMoreToRead() const {
    // If we haven't started the first block yet or are done our block, get the next block.
    return buffer != nullptr && !finishedBlock();
}

uint64_t ParallelCSVReader::parseBlock(
    common::block_idx_t blockIdx, common::DataChunk& resultChunk) {
    currentBlockIdx = blockIdx;
    seekToBlockStart();
    if (blockIdx == 0) {
        readBOM();
        if (csvReaderConfig.hasHeader) {
            readHeader();
        }
    }
    if (finishedBlock()) {
        return 0;
    }
    ParallelParsingDriver driver(resultChunk, this);
    return parseCSV(driver);
}

uint64_t ParallelCSVReader::continueBlock(common::DataChunk& resultChunk) {
    KU_ASSERT(hasMoreToRead());
    ParallelParsingDriver driver(resultChunk, this);
    return parseCSV(driver);
}

void ParallelCSVReader::seekToBlockStart() {
    // Seek to the proper location in the file.
    if (lseek(fd, currentBlockIdx * CopyConstants::PARALLEL_BLOCK_SIZE, SEEK_SET) == -1) {
        // LCOV_EXCL_START
        throw CopyException(stringFormat("Failed to seek to block {} in file {}: {}",
            currentBlockIdx, filePath, posixErrMessage()));
        // LCOV_EXCL_STOP
    }

    if (currentBlockIdx == 0) {
        // First block doesn't search for a newline.
        return;
    }

    // Reset the buffer.
    position = 0;
    bufferSize = 0;
    buffer.reset();
    if (!readBuffer(nullptr)) {
        return;
    }

    // Find the start of the next line.
    do {
        for (; position < bufferSize; position++) {
            if (buffer[position] == '\r') {
                position++;
                if (!maybeReadBuffer(nullptr)) {
                    return;
                }
                if (buffer[position] == '\n') {
                    position++;
                }
                return;
            } else if (buffer[position] == '\n') {
                position++;
                return;
            }
        }
    } while (readBuffer(nullptr));
}

void ParallelCSVReader::handleQuotedNewline() {
    throw CopyException(stringFormat("Quoted newlines are not supported in parallel CSV reader "
                                     "(while parsing {} on line {}). Please "
                                     "specify PARALLEL=FALSE in the options.",
        filePath, getLineNumber()));
}

bool ParallelCSVReader::finishedBlock() const {
    // Only stop if we've ventured into the next block by at least a byte.
    // Use `>` because `position` points to just past the newline right now.
    return getFileOffset() > (currentBlockIdx + 1) * CopyConstants::PARALLEL_BLOCK_SIZE;
}

function::function_set ParallelCSVScan::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<TableFunction>(READ_CSV_PARALLEL_FUNC_NAME, tableFunc, bindFunc,
            initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

void ParallelCSVScan::tableFunc(TableFunctionInput& input, common::DataChunk& outputChunk) {
    auto parallelCSVLocalState = reinterpret_cast<ParallelCSVLocalState*>(input.localState);
    auto parallelCSVSharedState = reinterpret_cast<ParallelCSVScanSharedState*>(input.sharedState);
    do {
        if (parallelCSVLocalState->reader != nullptr &&
            parallelCSVLocalState->reader->hasMoreToRead()) {
            auto result = parallelCSVLocalState->reader->continueBlock(outputChunk);
            outputChunk.state->selVector->selectedSize = result;
            if (result > 0) {
                return;
            }
        }
        auto [fileIdx, blockIdx] = parallelCSVSharedState->getNext();
        if (fileIdx == UINT64_MAX) {
            return;
        }
        if (fileIdx != parallelCSVLocalState->fileIdx) {
            parallelCSVLocalState->fileIdx = fileIdx;
            parallelCSVLocalState->reader = std::make_unique<ParallelCSVReader>(
                parallelCSVSharedState->readerConfig.filePaths[fileIdx],
                parallelCSVSharedState->readerConfig);
        }
        auto numRowsRead = parallelCSVLocalState->reader->parseBlock(blockIdx, outputChunk);
        outputChunk.state->selVector->selectedSize = numRowsRead;
        if (numRowsRead > 0) {
            return;
        }
        if (parallelCSVLocalState->reader->isEOF()) {
            parallelCSVSharedState->moveToNextFile(parallelCSVLocalState->fileIdx);
            parallelCSVLocalState->reader = nullptr;
        }
    } while (true);
}

} // namespace processor
} // namespace kuzu
