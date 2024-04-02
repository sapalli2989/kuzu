#pragma once

#include "processor/operator/hash_join/join_hash_table.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class MergeResultCollectorSharedState {
public:
    explicit MergeResultCollectorSharedState(std::unique_ptr<JoinHashTable> hashTable)
        : hashTable{std::move(hashTable)} {};

    virtual ~MergeResultCollectorSharedState() = default;

    void mergeLocalHashTable(JoinHashTable& localHashTable) {
        std::unique_lock lck(mtx);
        hashTable->merge(localHashTable);
    }

    inline JoinHashTable* getHashTable() { return hashTable.get(); }

protected:
    std::mutex mtx;
    std::unique_ptr<JoinHashTable> hashTable;
};

struct MergeBuildInfo {

public:
    MergeBuildInfo(std::vector<DataPos> keysPos, std::vector<common::FStateType> fStateTypes,
        std::vector<DataPos> payloadsPos, std::unique_ptr<FactorizedTableSchema> tableSchema)
        : keysPos{std::move(keysPos)}, fStateTypes{std::move(fStateTypes)},
          payloadsPos{std::move(payloadsPos)}, tableSchema{std::move(tableSchema)} {}
    MergeBuildInfo(const MergeBuildInfo& other)
        : keysPos{other.keysPos}, fStateTypes{other.fStateTypes}, payloadsPos{other.payloadsPos},
          tableSchema{other.tableSchema->copy()} {}

    inline uint32_t getNumKeys() const { return keysPos.size(); }

    inline FactorizedTableSchema* getTableSchema() const { return tableSchema.get(); }

    inline std::unique_ptr<MergeBuildInfo> copy() const {
        return std::make_unique<MergeBuildInfo>(*this);
    }

private:
    std::vector<DataPos> keysPos;
    std::vector<common::FStateType> fStateTypes;
    std::vector<DataPos> payloadsPos;
    std::unique_ptr<FactorizedTableSchema> tableSchema;
};

class MergeResultCollector : public Sink {
public:
    MergeResultCollector(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<MergeResultCollectorSharedState> sharedState,
        std::unique_ptr<MergeBuildInfo> info, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : MergeBuild{std::move(resultSetDescriptor), PhysicalOperatorType::MERGE_RESULT_COLLECTOR,
              std::move(sharedState), std::move(info), std::move(child), id, paramsString} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;
    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<MergeResultCollector>(resultSetDescriptor->copy(), sharedState,
            info->copy(), children[0]->clone(), id, paramsString);
    }

protected:
    virtual inline void appendVectors() {
        hashTable->appendVectors(keyVectors, payloadVectors, keyState);
    }

private:
    void setKeyState(common::DataChunkState* state);

protected:
    std::shared_ptr<HashJoinSharedState> sharedState;
    std::unique_ptr<HashJoinBuildInfo> info;

    std::vector<common::ValueVector*> keyVectors;
    // State of unFlat key(s). If all keys are flat, it points to any flat key state.
    common::DataChunkState* keyState = nullptr;
    std::vector<common::ValueVector*> payloadVectors;

    std::unique_ptr<JoinHashTable> hashTable; // local state
};

} // namespace processor
} // namespace kuzu
