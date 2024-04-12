#include "processor/operator/scan/scan_rel_table.h"

#include <iostream>
namespace kuzu {
namespace processor {

bool ScanRelTable::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        // 如果csr 越界了，我们需要重新读取vector的信息
        if(info->table->overFlowRescan(*scanState)){
            std::cout<<"=========overflow rescan========"<<std::endl;
            info->table->readBatch(context->clientContext->getTx(), *scanState);
        }
        if (scanState->hasMoreToRead(context->clientContext->getTx())) {
            std::cout<<"=========has more to read========"<<std::endl;
            info->table->updateResultPos(context->clientContext->getTx(), *scanState);
            return true;
        }
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        info->table->initializeReadState(context->clientContext->getTx(), info->direction,
            info->columnIDs, *inVector, *scanState);
        //TODO(Jimain): cache all vectors in outputVectors and then reset read scan state
        // 如果initilize的state是在一个新的batch vector里面，我们需要重新读取
        if(info->table->needRescan(*scanState)){
            std::cout<<"=========need rescan========"<<std::endl;
            info->table->initializeBatchReadState(info->direction,*scanState);
            info->table->readBatch(context->clientContext->getTx(), *scanState);
        }
    }
}

} // namespace processor
} // namespace kuzu
