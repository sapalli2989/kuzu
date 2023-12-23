#include "binder/binder.h"
#include "binder/bound_load_statement.h"
#include "common/cast.h"
#include "parser/load_statement.h"

using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindLoad(const Statement& statement) {
    auto loadStatement = common::ku_dynamic_cast<const Statement&, const LoadStatement&>(statement);
    return std::make_unique<BoundLoadStatement>(loadStatement.getPath());
}

} // namespace binder
} // namespace kuzu
