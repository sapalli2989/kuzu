#include "processor/operator/scan/scan_rel_table.h"

#include <iostream>
namespace kuzu {
namespace processor {

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (scanState->hasMoreToRead(context->clientContext->getTx())) {
            // check if we access the overflowed CSR offsets in the current cached batch vector
            // if so, we need to Rescan and cache a new batch vector
            if(scanState->checkOverFlowedCSR()){
                info->table->readBatch(context->clientContext->getTx(), *scanState);
            }
            info->table->updateResultPos(context->clientContext->getTx(), *scanState);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        info->table->initializeReadState(context->clientContext->getTx(), info->direction,
            info->columnIDs, *inVector, *scanState);
        // cache every default_vector_size csr offsets vectors in outputVectors
        // needRescan() will check if we need batch read a new vector
        // initializeBatchReadState() will reset lastPosInCSR to 0 (since we are in new vector, the lastPosInCSR should be 0)
        if(scanState->needRescan()){
            scanState->initializeBatchReadState();
            info->table->readBatch(context->clientContext->getTx(), *scanState);
        }
    }
}

} // namespace processor
} // namespace kuzu
