#include "processor/operator/persistent/index_builder.h"

#include <thread>

#include "common/cast.h"
#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "storage/store/string_column_chunk.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::storage;

IndexBuilderGlobalQueues::IndexBuilderGlobalQueues(std::unique_ptr<PrimaryKeyIndexBuilder> pkIndex)
    : nextID(0), pkIndex(std::move(pkIndex)) {
    if (this->pkIndex->keyTypeID() == LogicalTypeID::STRING) {
        queues.emplace<StringQueues>();
    } else {
        queues.emplace<IntQueues>();
    }
}

void IndexBuilderGlobalQueues::consume(size_t id) {
    std::shared_lock slck{rwlock};
    auto queueRange = consumeRanges[id];
    slck.unlock();

    return consumeRange(queueRange);
}

void IndexBuilderGlobalQueues::consumeAll() {
    return consumeRange(std::make_pair(0, NUM_HASH_INDEXES));
}

void IndexBuilderGlobalQueues::consumeRange(std::pair<size_t, size_t> queueRange) {
    StringQueues* stringQueues = std::get_if<StringQueues>(&queues);
    if (stringQueues) {
        for (auto indexPos = queueRange.first; indexPos < queueRange.second; indexPos++) {
            if (!consumeLocks[indexPos].try_lock()) {
                continue;
            }
            std::unique_lock mtx{consumeLocks[indexPos], std::adopt_lock};

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
            if (!consumeLocks[indexPos].try_lock()) {
                continue;
            }
            std::unique_lock mtx{consumeLocks[indexPos], std::adopt_lock};

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
    std::unique_lock xlck{rwlock};
    auto id = nextID++;
    if (id == 0) {
        consumeRanges.emplace_back(0, NUM_HASH_INDEXES);
        nextSplit = 0;
        lastSplit = 1;
    } else {
        auto [lo, hi] = consumeRanges[nextSplit];
        auto mid = (lo + hi) / 2;
        consumeRanges[nextSplit] = {lo, mid};
        consumeRanges.emplace_back(mid, hi);

        nextSplit++;
        if (nextSplit == lastSplit) {
            nextSplit = 0;
            lastSplit *= 2;
        }
    }
    return id;
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

void IndexBuilderSharedState::producerStart() {
    producers.fetch_add(1, std::memory_order_relaxed);
}

void IndexBuilderSharedState::producerQuit() {
    if (producers.fetch_sub(1, std::memory_order_relaxed) == 1) {
        done.store(true, std::memory_order_relaxed);
    }
}

IndexBuilder::IndexBuilder(std::shared_ptr<IndexBuilderSharedState> sharedState)
    : sharedState(std::move(sharedState)), consumer(this->sharedState->globalQueues),
      localBuffers(this->sharedState->globalQueues) {}

void IndexBuilderSharedState::init() {}

IndexBuilder::IndexBuilder(std::unique_ptr<PrimaryKeyIndexBuilder> pkIndex)
    : IndexBuilder(std::make_shared<IndexBuilderSharedState>(std::move(pkIndex))) {}

void IndexBuilder::initLocalStateInternal(ExecutionContext* /*context*/) {
    sharedState->producerStart();
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
    sharedState->producerQuit();

    consume();
    while (!sharedState->isDone()) {
        std::this_thread::sleep_for(std::chrono::microseconds(500));
        consume();
    }
}

void IndexBuilder::finalize(ExecutionContext* /*context*/) {
    localBuffers.flush();
    sharedState->consumeAll();
    // sharedState->flush();
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
