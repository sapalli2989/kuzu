#include "processor/operator/call/in_query_call.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void InQueryCall::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    localState = std::make_unique<InQueryCallLocalState>();
    localState->outputChunk = std::make_unique<DataChunk>(inQueryCallInfo->outputPoses.size(),
        resultSet->getDataChunk(inQueryCallInfo->outputPoses[0].dataChunkPos)->state);
    for (auto i = 0u; i < inQueryCallInfo->outputPoses.size(); i++) {
        localState->outputChunk->insert(
            i, resultSet->getValueVector(inQueryCallInfo->outputPoses[i]));
    }
    localState->rowIDVector = resultSet->getValueVector(inQueryCallInfo->rowIDPos).get();
    function::TableFunctionInitInput tableFunctionInitInput{inQueryCallInfo->bindData.get()};
    localState->localState = inQueryCallInfo->function->initLocalStateFunc(
        tableFunctionInitInput, sharedState->sharedState.get());
}

void InQueryCall::initGlobalStateInternal(ExecutionContext* /*context*/) {
    function::TableFunctionInitInput tableFunctionInitInput{inQueryCallInfo->bindData.get()};
    sharedState->sharedState =
        inQueryCallInfo->function->initSharedStateFunc(tableFunctionInitInput);
}

bool InQueryCall::getNextTuplesInternal(ExecutionContext* /*context*/) {
    function::TableFunctionInput tableFunctionInput{inQueryCallInfo->bindData.get(),
        localState->localState.get(), sharedState->sharedState.get()};
    localState->outputChunk->state->selVector->selectedSize = 0;
    localState->outputChunk->resetAuxiliaryBuffer();
    inQueryCallInfo->function->tableFunc(tableFunctionInput, *localState->outputChunk);
    auto numRowsToOutput = localState->outputChunk->state->selVector->selectedSize;
    auto rowIdx = sharedState->getAndIncreaseRowIdx(numRowsToOutput);
    for (auto i = 0u; i < numRowsToOutput; i++) {
        localState->rowIDVector->setValue(i, rowIdx + i);
    }
    return localState->outputChunk->state->selVector->selectedSize != 0;
}

} // namespace processor
} // namespace kuzu
