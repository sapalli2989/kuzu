#include <variant>

#include "common/static_vector.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "processor/execution_context.h"
#include "processor/operator/persistent/mpsc_queue.h"
#include "storage/index/hash_index_builder.h"
#include "storage/store/column_chunk.h"
#include <latch>

namespace kuzu {
namespace processor {

constexpr size_t BUFFER_SIZE = 1024;
using IntBuffer = common::StaticVector<std::pair<int64_t, common::offset_t>, BUFFER_SIZE>;
using StringBuffer = common::StaticVector<std::pair<std::string, common::offset_t>, BUFFER_SIZE>;

class IndexBuilderGlobalQueues {
public:
    explicit IndexBuilderGlobalQueues(std::unique_ptr<storage::PrimaryKeyIndexBuilder> pkIndex);

    void consume(std::pair<size_t, size_t> queueRange);
    void flushToDisk() const;

    void insert(size_t index, StringBuffer elem) {
        std::get<StringQueues>(queues)[index].push(std::move(elem));
    }
    void insert(size_t index, IntBuffer elem) {
        std::get<IntQueues>(queues)[index].push(std::move(elem));
    }

    common::LogicalTypeID pkTypeID() const { return pkIndex->keyTypeID(); }

private:
    std::unique_ptr<storage::PrimaryKeyIndexBuilder> pkIndex;

    using StringQueues = std::array<MPSCQueue<StringBuffer>, storage::NUM_HASH_INDEXES>;
    using IntQueues = std::array<MPSCQueue<IntBuffer>, storage::NUM_HASH_INDEXES>;

    // Queues for distributing primary keys.
    std::variant<StringQueues, IntQueues> queues;
};

class IndexBuilderConsumer {
public:
    explicit IndexBuilderConsumer(IndexBuilderGlobalQueues& globalQueues);

    void init(size_t seq, uint64_t numThreads);
    void consume();

private:
    IndexBuilderGlobalQueues* globalQueues;
    std::pair<size_t, size_t> queueRange;
};

class IndexBuilderLocalBuffers {
public:
    explicit IndexBuilderLocalBuffers(IndexBuilderGlobalQueues& globalQueues);

    void insert(std::string key, common::offset_t value);
    void insert(int64_t key, common::offset_t value);

    void flush();

private:
    IndexBuilderGlobalQueues* globalQueues;

    using StringBuffers = std::array<StringBuffer, storage::NUM_HASH_INDEXES>;
    using IntBuffers = std::array<IntBuffer, storage::NUM_HASH_INDEXES>;
    std::variant<StringBuffers, IntBuffers> buffers;
};

class IndexBuilderSharedState {
    friend class IndexBuilder;

public:
    explicit IndexBuilderSharedState(std::unique_ptr<storage::PrimaryKeyIndexBuilder> pkIndex);
    void init(uint64_t numThreads);
    void flush() { globalQueues.flushToDisk(); }

private:
    size_t nextSeq() { return seq.fetch_add(1, std::memory_order_relaxed); }

    IndexBuilderGlobalQueues globalQueues;

    // Atomic for distributing ranges to each operator.
    std::atomic<size_t> seq;
    std::unique_ptr<std::latch> latch;
};

class IndexBuilder {
    // PassKey: only we can create instances of this class.
    // This means only we can call the below constructor, but we can let make_unique
    // call it (unlike if it were private).
    struct PassKey {};

public:
    explicit IndexBuilder(std::shared_ptr<IndexBuilderSharedState> sharedState, PassKey key);
    explicit IndexBuilder(std::unique_ptr<storage::PrimaryKeyIndexBuilder> pkIndex);

    std::unique_ptr<IndexBuilder> clone() {
        return std::make_unique<IndexBuilder>(sharedState, PassKey());
    }

    void initGlobalStateInternal(ExecutionContext* /*context*/) {}
    void initLocalStateInternal(ExecutionContext* context);
    void consume() { consumer.consume(); }
    void insert(
        storage::ColumnChunk* chunk, common::offset_t nodeOffset, common::offset_t numNodes);
    void flushLocalBuffers() { localBuffers.flush(); }
    void producingFinished();

private:
    void checkNonNullConstraint(storage::NullColumnChunk* nullChunk, common::offset_t numNodes);
    std::shared_ptr<IndexBuilderSharedState> sharedState;

    IndexBuilderConsumer consumer;
    IndexBuilderLocalBuffers localBuffers;
};

} // namespace processor
} // namespace kuzu
