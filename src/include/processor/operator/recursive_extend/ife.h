#pragma once

#include "function/hash/hash_functions.h"
#include "processor/operator/physical_operator.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

struct IFEVectors;

template<typename T>
using node_id_map_t = std::unordered_map<common::nodeID_t, T, function::InternalIDHasher>;
using edge_info_t = std::pair<common::nodeID_t, common::relID_t>;

struct Frontier {
    std::vector<common::nodeID_t> nodeIDs;
    node_id_map_t<std::vector<edge_info_t>> edgeInfosMap;

    common::offset_t size() const { return nodeIDs.size(); }

    common::nodeID_t getNodeID(common::idx_t idx) const { return nodeIDs[idx]; }

    void insert(common::nodeID_t srcNodeID, common::relID_t edgeID, common::nodeID_t dstNodeID);

    void merge(const Frontier& other);
};

struct RJGraphScanState {};

struct RJGraph {
    std::vector<Frontier> frontiers;
};

struct RJGraphLevelScanner {
    RJGraph* graph;
    uint8_t level;

    void scan(common::offset_t startOffset);
};

// It's unclear if we can
struct FrontierMorsel {
    Frontier* frontier;
    common::offset_t offset;

    bool isEmpty() const { return offset == common::INVALID_OFFSET; }
};

struct IFESourceState {
    std::mutex mtx;

    uint32_t numComputeThreadsRegistered = 0;
    uint32_t numComputeThreadsFinished = 0;

    RJGraph graph;
    uint8_t currentLevel = 0;
    common::offset_t currentFrontierCursor = 0;

    IFESourceState() : graph{} {}
    DELETE_COPY_AND_MOVE(IFESourceState);

    bool isRJGraphComplete();

    FrontierMorsel getFrontierMorsel();

    void registerNewThread() { numComputeThreadsRegistered++; }
    void finalizeFrontier(const Frontier& frontier);
};

struct IFESharedState {
    std::mutex mtx;

    std::shared_ptr<FactorizedTable> inputTable;
    common::offset_t tableCursor;

    std::vector<std::shared_ptr<IFESourceState>> activeSourceStates;

    IFESourceState* getSourceState();
};

struct IFEVectors {

    common::ValueVector* recursiveEdgeIDVector = nullptr;
    common::ValueVector* recursiveDstNodeIDVector = nullptr;
};

class IFEScan;
class IFE : public PhysicalOperator {
public:
    bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet*, ExecutionContext*) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

private:
    void scanRJGraph();

    void computeRJGraph(ExecutionContext* context);

private:
    std::shared_ptr<IFESharedState> sharedState;
    IFESourceState* activeSourceState;

    IFEVectors vectors;

    IFEScan* ifeScan;
    PhysicalOperator* ifePipelineRoot;
};

class IFEScan : public PhysicalOperator {
public:
    IFEScan(DataPos idPos, operator_id_t id, const std::string& paramStr)
        : PhysicalOperator{PhysicalOperatorType::SCAN_NODE_ID, id, paramStr}, idPos{idPos} {}

    void setNodeID(common::nodeID_t nodeID);

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext*) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<IFEScan>(idPos, id, paramsString);
    }

private:
    DataPos idPos;
    common::ValueVector* idVector;
    bool executed = false;
};

} // namespace processor
} // namespace kuzu
