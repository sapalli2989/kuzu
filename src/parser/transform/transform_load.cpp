
#include "parser/load_statement.h"
#include "parser/transformer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformLoad(CypherParser::KU_LoadContext& ctx) {
    return std::make_unique<LoadStatement>(transformStringLiteral(*ctx.StringLiteral()));
}

} // namespace parser
} // namespace kuzu
