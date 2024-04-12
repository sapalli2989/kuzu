#include "processor/operator/scan/scan_rel_table.h"
#include "storage/store/table.h"

#include <iostream>
namespace kuzu {
namespace processor {

    //innode vector:[1,2,3,4,5]
    // [1]
    // [11,12,13,14,15,xxx,2047] [1,2,3,4,5]
    // [11] [1]

    // [2]
    // [12]
bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (scanState->hasMoreToRead(context->clientContext->getTx())) {
            info->table->updateResultPos(context->clientContext->getTx(), *scanState);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        info->table->initializeReadState(context->clientContext->getTx(), info->direction,
            info->columnIDs, *inVector, *scanState);
        //TODO(Jimain): cache all vectors in outputVectors and then reset read scan state
        if(info->table->needRescan(*scanState)){
            info->table->initializeBatchReadState(info->direction,*scanState);
            info->table->readBatch(context->clientContext->getTx(), *scanState);
        }
    }
}

} // namespace processor
} // namespace kuzu
