#pragma once

#include <vector>

#include "common/types/value/value.h"

namespace kuzu {
namespace function {

struct TableFuncBindInput {
    explicit TableFuncBindInput(std::vector<std::unique_ptr<common::Value>> inputs)
        : inputs{std::move(inputs)} {}
    TableFuncBindInput() = default;
    std::vector<std::unique_ptr<common::Value>> inputs;
};

struct ScanTableFuncBindInput final : public function::TableFuncBindInput {
    ScanTableFuncBindInput(const common::ReaderConfig config, storage::MemoryManager* mm)
        : config{config}, mm{mm} {
        inputs.push_back(
            std::make_unique<common::Value>(common::Value::createValue(this->config.filePaths[0])));
    }
    const common::ReaderConfig config;
    storage::MemoryManager* mm;
};

} // namespace function
} // namespace kuzu
