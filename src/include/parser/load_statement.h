#pragma once

#include <string>

#include "statement.h"

namespace kuzu {
namespace parser {

class LoadStatement final : public Statement {
public:
    explicit LoadStatement(std::string path)
        : Statement{common::StatementType::LOAD}, path{std::move(path)} {}

    inline std::string getPath() const { return path; }

private:
    std::string path;
};

} // namespace parser
} // namespace kuzu
