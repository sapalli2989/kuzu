#include "function/algorithm/on_disk_graph.h"

#include "common/enums/table_type.h"
#include "function/algorithm/graph.h"

using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace graph {

NbrScanState::NbrScanState(storage::MemoryManager* mm) {
    srcNodeIDVectorState = DataChunkState::getSingleValueDataChunkState();
    dstNodeIDVectorState = std::make_shared<common::DataChunkState>();
    srcNodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
    srcNodeIDVector->state = srcNodeIDVectorState;
    dstNodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
    dstNodeIDVector->state = dstNodeIDVectorState;
    outputNodeIDVectors = std::vector<ValueVector*>();
    outputNodeIDVectors.push_back(dstNodeIDVector.get());
    fwdReadState = std::make_unique<RelTableReadState>(*srcNodeIDVector, columnIDs,
        outputNodeIDVectors, direction);
}

OnDiskGraph::OnDiskGraph(ClientContext* context, const std::string& nodeTableName,
    const std::string& relTableName)
    : Graph{context} {
    auto catalog = context->getCatalog();
    auto storage = context->getStorageManager();
    auto tx = context->getTx();
    if (!nodeTableName.empty()) {
        auto nodeTableID = catalog->getTableID(tx, nodeTableName);
        nodeTable = ku_dynamic_cast<Table*, NodeTable*>(storage->getTable(nodeTableID));
    }
    if (!relTableName.empty()) {
        auto relTableID = catalog->getTableID(tx, relTableName);
        relTable = ku_dynamic_cast<Table*, RelTable*>(storage->getTable(relTableID));
    }
}

common::offset_t OnDiskGraph::getNumNodes() {
    return nodeTable->getNumTuples(context->getTx());
}

common::offset_t OnDiskGraph::getNumEdges() {
    return relTable->getNumTuples(context->getTx());
}

uint64_t OnDiskGraph::getFwdDegreeOffset(common::offset_t offset) {
    return relTable->getDirectedTableData(common::RelDataDirection::FWD)
        ->getNodeRels(context->getTx(), offset);
}

void OnDiskGraph::initializeStateFwdNbrs(common::offset_t offset, NbrScanState *nbrScanState) {
    nbrScanState->srcNodeIDVector->setValue<nodeID_t>(0, {offset, nodeTable->getTableID()});
    relTable->initializeReadState(context->getTx(), nbrScanState->direction,
        nbrScanState->columnIDs, *nbrScanState->srcNodeIDVector, *nbrScanState->fwdReadState.get());
}

bool OnDiskGraph::hasMoreFwdNbrs(NbrScanState *nbrScanState) {
    return nbrScanState->fwdReadState->hasMoreToRead(context->getTx());
}

common::ValueVector* OnDiskGraph::getFwdNbrs(NbrScanState *nbrScanState) {
    auto tx = context->getTx();
    auto readState = nbrScanState->fwdReadState.get();
    relTable->read(tx, *readState);
    return nbrScanState->outputNodeIDVectors[0];
}

} // namespace graph
} // namespace kuzu