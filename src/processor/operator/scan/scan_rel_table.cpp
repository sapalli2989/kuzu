#include "processor/operator/scan/scan_rel_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void ScanRelTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* executionContext) {
    ScanTable::initLocalStateInternal(resultSet, executionContext);
    assert(info);
    if (info) {
        ValueVector* directionVector;
        if (info->directionPos.isValid()) {
            directionVector = resultSet->getValueVector(info->directionPos).get();
        }
        scanState = std::make_unique<storage::RelTableReadState>(*inVector, info->columnIDs,
            outVectors, info->direction, directionVector);
    }
}

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (scanState->hasMoreToRead(context->clientContext->getTx())) {
            info->table->read(context->clientContext->getTx(), *scanState);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        info->table->initializeReadState(context->clientContext->getTx(), info->direction,
            info->columnIDs, *inVector, *scanState);
    }
}

} // namespace processor
} // namespace kuzu
