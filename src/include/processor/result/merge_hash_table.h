#include "processor/operator/aggregate/aggregate_hash_table.h"

namespace kuzu {
namespace processor {

class MergeHashTable : public AggregateHashTable {

public:
    uint64_t matchFTEntries(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors, uint64_t numMayMatches,
        uint64_t numNoMatches) override {
        auto colIdx = 0u;
        for (auto& flatKeyVector : flatKeyVectors) {
            numMayMatches =
                matchFlatVecWithFTColumn(flatKeyVector, numMayMatches, numNoMatches, colIdx++);
        }
        for (auto& unFlatKeyVector : unFlatKeyVectors) {
            numMayMatches =
                matchUnFlatVecWithFTColumn(unFlatKeyVector, numMayMatches, numNoMatches, colIdx++);
        }
        for (auto i = 0u; i < numMayMatches; i++) {
            onMatchSlotIdxes.emplace(mayMatchIdxes[i]);
        }
        return numNoMatches;
    }

    void initializeFTEntries(const std::vector<common::ValueVector*>& flatKeyVectors,
        const std::vector<common::ValueVector*>& unFlatKeyVectors,
        const std::vector<common::ValueVector*>& dependentKeyVectors,
        uint64_t numFTEntriesToInitialize) override {
        AggregateHashTable::initializeFTEntries(
            flatKeyVectors, unFlatKeyVectors, dependentKeyVectors, numFTEntriesToInitialize);
        for (auto i = 0u; i < numFTEntriesToInitialize; i++) {
            auto entryIdx = entryIdxesToInitialize[i];
            auto entry = hashSlotsToUpdateAggState[entryIdx]->entry;
            auto onMatch = onMatchSlotIdxes.contains(entryIdx);
            onMatchSlotIdxes.erase(entryIdx);
            factorizedTable->updateFlatCellNoNull(
                entry, onMatchColIdxInFT, &onMatch /* isOnMatch */);
        }
    }

    void initializeFT(
        const std::vector<std::unique_ptr<function::AggregateFunction>>& aggFuncs) override {
        auto isUnflat = false;
        auto dataChunkPos = 0u;
        std::unique_ptr<FactorizedTableSchema> tableSchema =
            std::make_unique<FactorizedTableSchema>();
        aggStateColIdxInFT = keyTypes.size() + dependentKeyDataTypes.size();
        for (auto& dataType : keyTypes) {
            auto size = common::LogicalTypeUtils::getRowLayoutSize(dataType);
            tableSchema->appendColumn(std::make_unique<ColumnSchema>(isUnflat, dataChunkPos, size));
            numBytesForKeys += size;
        }
        for (auto& dataType : dependentKeyDataTypes) {
            auto size = common::LogicalTypeUtils::getRowLayoutSize(dataType);
            tableSchema->appendColumn(std::make_unique<ColumnSchema>(isUnflat, dataChunkPos, size));
            numBytesForDependentKeys += size;
        }
        aggStateColOffsetInFT = numBytesForKeys + numBytesForDependentKeys;

        aggregateFunctions.reserve(aggFuncs.size());
        updateAggFuncs.reserve(aggFuncs.size());
        for (auto i = 0u; i < aggFuncs.size(); i++) {
            auto& aggFunc = aggFuncs[i];
            tableSchema->appendColumn(std::make_unique<ColumnSchema>(
                isUnflat, dataChunkPos, aggFunc->getAggregateStateSize()));
            aggregateFunctions.push_back(aggFunc->clone());
            updateAggFuncs.push_back(aggFunc->isFunctionDistinct() ?
                                         &AggregateHashTable::updateDistinctAggState :
                                         &AggregateHashTable::updateAggState);
        }
        tableSchema->appendColumn(
            std::make_unique<ColumnSchema>(isUnflat, dataChunkPos, sizeof(bool)));
        onMatchColIdxInFT = aggStateColIdxInFT + aggFuncs.size();
        tableSchema->appendColumn(
            std::make_unique<ColumnSchema>(isUnflat, dataChunkPos, sizeof(common::hash_t)));
        hashColIdxInFT = onMatchColIdxInFT + 1;
        hashColOffsetInFT = tableSchema->getColOffset(hashColIdxInFT);
        factorizedTable = std::make_unique<FactorizedTable>(&memoryManager, std::move(tableSchema));
    }

private:
    std::unordered_set<uint64_t> onMatchSlotIdxes;
    uint32_t onMatchColIdxInFT;
};

} // namespace processor
} // namespace kuzu
