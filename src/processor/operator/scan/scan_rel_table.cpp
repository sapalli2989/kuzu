#include "processor/operator/scan/scan_rel_table.h"

#include <iostream>
namespace kuzu {
namespace processor {

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
            std::cout<<"=========rescan========"<<std::endl;
            info->table->readBatch(context->clientContext->getTx(), *scanState);
        }
    }
}

} // namespace processor
} // namespace kuzu
