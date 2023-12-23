#pragma once

#include "bound_statement.h"

namespace kuzu {
namespace binder {

class BoundLoadStatement final : public BoundStatement {
public:
    explicit BoundLoadStatement(std::string path)
        : BoundStatement{common::StatementType::LOAD, BoundStatementResult::createEmptyResult()},
          path{std::move(path)} {}

    inline std::string getPath() const { return path; }

private:
    std::string path;
};

} // namespace binder
} // namespace kuzu
