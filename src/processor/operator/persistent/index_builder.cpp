#include "processor/operator/persistent/index_builder.h"

#include "common/cast.h"
#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "storage/store/string_column_chunk.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::storage;

IndexBuilderGlobalQueues::IndexBuilderGlobalQueues(std::unique_ptr<PrimaryKeyIndexBuilder> pkIndex)
    : pkIndex(std::move(pkIndex)), nextID(0) {
    if (this->pkIndex->keyTypeID() == LogicalTypeID::STRING) {
        queues.emplace<StringQueues>();
    } else {
        queues.emplace<IntQueues>();
    }
}

void IndexBuilderGlobalQueues::consume(size_t id) {
    std::shared_lock slck{mtx};
    auto queueRange = consumeRanges[id];

    StringQueues* stringQueues = std::get_if<StringQueues>(&queues);
    if (stringQueues) {
        for (auto indexPos = queueRange.first; indexPos < queueRange.second; indexPos++) {
            StringBuffer elem;
            while ((*stringQueues)[indexPos].pop(elem)) {
                for (auto i = 0u; i < elem.size(); i++) {
                    auto [key, value] = elem[i];
                    if (!pkIndex->appendWithIndexPos(key.c_str(), value, indexPos)) {
                        throw CopyException(ExceptionMessage::existedPKException(std::move(key)));
                    }
                }
            }
        }
    } else {
        IntQueues& intQueues = std::get<IntQueues>(queues);
        for (auto indexPos = queueRange.first; indexPos < queueRange.second; indexPos++) {
            IntBuffer elem;
            while (intQueues[indexPos].pop(elem)) {
                for (auto i = 0u; i < elem.size(); i++) {
                    auto [key, value] = elem[i];
                    if (!pkIndex->appendWithIndexPos(key, value, indexPos)) {
                        throw CopyException(
                            ExceptionMessage::existedPKException(std::to_string(key)));
                    }
                }
            }
        }
    }
}

void IndexBuilderGlobalQueues::flushToDisk() const {
    pkIndex->flush();
}

size_t IndexBuilderGlobalQueues::addWorker() {
    std::unique_lock xlck{mtx};
    auto id = nextID++;
    activeWorkers.push_back(id);
    consumeRanges.emplace_back();
    updateRangesNoLock();
    return id;
}

void IndexBuilderGlobalQueues::workerQuit(size_t id) {
    std::unique_lock xlck{mtx};
    auto it = std::find(activeWorkers.begin(), activeWorkers.end(), id);
    KU_ASSERT(it != activeWorkers.end());
    activeWorkers.erase(it);
    updateRangesNoLock();
}

void IndexBuilderGlobalQueues::updateRangesNoLock() {
    auto workerCount = activeWorkers.size();
    if (workerCount == 0) {
        return;
    }

    auto indexesPerWorker = NUM_HASH_INDEXES / workerCount;
    auto remainingIndexes = NUM_HASH_INDEXES % workerCount;
    auto currentTot = 0;
    for (auto i = 0u; i < remainingIndexes; i++) {
        auto id = activeWorkers[i];
        consumeRanges[id].first = currentTot;
        currentTot += indexesPerWorker + 1;
        consumeRanges[id].second = currentTot;
    }
    for (auto i = remainingIndexes; i < workerCount; i++) {
        auto id = activeWorkers[i];
        consumeRanges[id].first = currentTot;
        currentTot += indexesPerWorker;
        consumeRanges[id].second = currentTot;
    }
}

IndexBuilderConsumer::IndexBuilderConsumer(IndexBuilderGlobalQueues& globalQueues)
    : globalQueues(&globalQueues) {}

void IndexBuilderConsumer::init() {
    id = globalQueues->addWorker();
}

IndexBuilderLocalBuffers::IndexBuilderLocalBuffers(IndexBuilderGlobalQueues& globalQueues)
    : globalQueues(&globalQueues) {
    if (globalQueues.pkTypeID() == LogicalTypeID::STRING) {
        stringBuffers = std::make_unique<StringBuffers>();
    } else {
        intBuffers = std::make_unique<IntBuffers>();
    }
}

void IndexBuilderLocalBuffers::insert(std::string key, common::offset_t value) {
    auto indexPos = getHashIndexPosition(key.c_str());
    if ((*stringBuffers)[indexPos].full()) {
        globalQueues->insert(indexPos, std::move((*stringBuffers)[indexPos]));
    }
    (*stringBuffers)[indexPos].push_back(std::make_pair(key, value));
}

void IndexBuilderLocalBuffers::insert(int64_t key, common::offset_t value) {
    auto indexPos = getHashIndexPosition(key);
    if ((*intBuffers)[indexPos].full()) {
        globalQueues->insert(indexPos, std::move((*intBuffers)[indexPos]));
    }
    (*intBuffers)[indexPos].push_back(std::make_pair(key, value));
}

void IndexBuilderLocalBuffers::flush() {
    if (globalQueues->pkTypeID() == LogicalTypeID::STRING) {
        for (auto i = 0u; i < stringBuffers->size(); i++) {
            globalQueues->insert(i, std::move((*stringBuffers)[i]));
        }
    } else {
        for (auto i = 0u; i < intBuffers->size(); i++) {
            globalQueues->insert(i, std::move((*intBuffers)[i]));
        }
    }
}

IndexBuilderSharedState::IndexBuilderSharedState(std::unique_ptr<PrimaryKeyIndexBuilder> pkIndex)
    : globalQueues(std::move(pkIndex)) {}

IndexBuilder::IndexBuilder(std::shared_ptr<IndexBuilderSharedState> sharedState)
    : sharedState(std::move(sharedState)), consumer(this->sharedState->globalQueues),
      localBuffers(this->sharedState->globalQueues) {}

void IndexBuilderSharedState::init() {}

IndexBuilder::IndexBuilder(std::unique_ptr<PrimaryKeyIndexBuilder> pkIndex)
    : IndexBuilder(std::make_shared<IndexBuilderSharedState>(std::move(pkIndex))) {}

void IndexBuilder::initLocalStateInternal(ExecutionContext* /*context*/) {
    consumer.init();
}

void IndexBuilder::insert(ColumnChunk* chunk, offset_t nodeOffset, offset_t numNodes) {
    checkNonNullConstraint(chunk->getNullChunk(), numNodes);

    switch (chunk->getDataType()->getPhysicalType()) {
    case PhysicalTypeID::INT64: {
        for (auto i = 0u; i < numNodes; i++) {
            auto value = chunk->getValue<int64_t>(i);
            localBuffers.insert(value, nodeOffset + i);
        }
    } break;
    case PhysicalTypeID::STRING: {
        auto stringColumnChunk = ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(chunk);
        for (auto i = 0u; i < numNodes; i++) {
            auto value = stringColumnChunk->getValue<std::string>(i);
            localBuffers.insert(std::move(value), nodeOffset + i);
        }
    } break;
    default: {
        throw CopyException(ExceptionMessage::invalidPKType(chunk->getDataType()->toString()));
    }
    }
}

void IndexBuilder::finishedProducing() {
    localBuffers.flush();
    consumer.quit();
}

void IndexBuilder::finalize(ExecutionContext* /*context*/) {
    localBuffers.flush();
    consumer.consume();
    sharedState->flush();
}

void IndexBuilder::checkNonNullConstraint(NullColumnChunk* nullChunk, offset_t numNodes) {
    for (auto i = 0u; i < numNodes; i++) {
        if (nullChunk->isNull(i)) {
            throw CopyException(ExceptionMessage::nullPKException());
        }
    }
}

} // namespace processor
} // namespace kuzu
