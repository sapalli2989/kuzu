#pragma once

#include "processor/result/result_set.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace evaluator {

class ExpressionEvaluator {
public:
    ExpressionEvaluator() = default;
    // Leaf evaluators (reference or literal)
    explicit ExpressionEvaluator(bool isResultFlat) : isResultFlat_{isResultFlat} {}
    explicit ExpressionEvaluator(std::vector<std::unique_ptr<ExpressionEvaluator>> children)
        : children{std::move(children)} {}
    virtual ~ExpressionEvaluator() = default;

    inline bool isResultFlat() const { return isResultFlat_; }

    virtual void init(const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager);

    virtual void evaluate(main::ClientContext* clientContext) = 0;

    virtual bool select(common::SelectionVector& selVector, main::ClientContext* clientContext) = 0;

    virtual std::unique_ptr<ExpressionEvaluator> clone() = 0;

    static std::vector<std::unique_ptr<ExpressionEvaluator>> copy(
        const std::vector<std::unique_ptr<ExpressionEvaluator>>& evaluators);

protected:
    virtual void resolveResultVector(const processor::ResultSet& resultSet,
        storage::MemoryManager* memoryManager) = 0;

    void resolveResultStateFromChildren(const std::vector<ExpressionEvaluator*>& inputEvaluators);

public:
    std::shared_ptr<common::ValueVector> resultVector;

protected:
    bool isResultFlat_ = true;
    std::vector<std::unique_ptr<ExpressionEvaluator>> children;
};

} // namespace evaluator
} // namespace kuzu
