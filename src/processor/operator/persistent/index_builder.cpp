#include "processor/operator/persistent/index_builder.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::storage;

IndexBuilderGlobalQueues::IndexBuilderGlobalQueues(std::unique_ptr<PrimaryKeyIndexBuilder> pkIndex)
    : pkIndex(std::move(pkIndex)) {
    if (this->pkIndex->keyTypeID() == LogicalTypeID::STRING) {
        queues.emplace<StringQueues>();
    } else {
        queues.emplace<IntQueues>();
    }
}

void IndexBuilderGlobalQueues::consume(std::pair<size_t, size_t> queueRange) {
    StringQueues* stringQueues = std::get_if<StringQueues>(&queues);
    if (stringQueues) {
        for (auto indexPos = queueRange.first; indexPos < queueRange.second; indexPos++) {
            StringBuffer elem;
            while ((*stringQueues)[indexPos].pop(elem)) {
                for (auto i = 0u; i < elem.size(); i++) {
                    auto [key, value] = elem[i];
                    pkIndex->appendWithIndexPos(key.c_str(), value, indexPos);
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
                    pkIndex->appendWithIndexPos(key, value, indexPos);
                }
            }
        }
    }
}

void IndexBuilderGlobalQueues::flushToDisk() const {
    pkIndex->flush();
}

IndexBuilderConsumer::IndexBuilderConsumer(IndexBuilderGlobalQueues& globalQueues)
    : globalQueues(&globalQueues) {}

void IndexBuilderConsumer::init(size_t seq, uint64_t numThreads) {
    // Use the sequence number to calculate the range of queues we are responsible for.
    auto queuesPerThread = NUM_HASH_INDEXES / numThreads;
    // If our sequence is before the remainder, take more queues.
    if (seq < NUM_HASH_INDEXES % numThreads) {
        queuesPerThread += 1;
        queueRange = std::make_pair(seq * queuesPerThread, (seq + 1) * queuesPerThread);
    } else {
        // Otherwise, count from the end.
        auto revSeq = numThreads - seq;
        queueRange = std::make_pair(NUM_HASH_INDEXES - revSeq * queuesPerThread,
            NUM_HASH_INDEXES - (revSeq - 1) * queuesPerThread);
    }
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

void IndexBuilderSharedState::init(uint64_t numThreads) {
    latch.emplace(numThreads);
}

IndexBuilder::IndexBuilder(std::unique_ptr<PrimaryKeyIndexBuilder> pkIndex)
    : IndexBuilder(std::make_shared<IndexBuilderSharedState>(std::move(pkIndex))) {}

void IndexBuilder::initLocalStateInternal(ExecutionContext* context) {
    consumer.init(sharedState->nextSeq(), context->numThreads);
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
        for (auto i = 0u; i < numNodes; i++) {
            auto value = chunk->getValue<std::string>(i);
            localBuffers.insert(value, nodeOffset + i);
        }
    } break;
    default: {
        throw CopyException(ExceptionMessage::invalidPKType(chunk->getDataType()->toString()));
    }
    }
}

void IndexBuilder::producingFinished() {
    sharedState->latch->arrive_and_wait();
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
