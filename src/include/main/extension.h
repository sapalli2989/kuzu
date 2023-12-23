#pragma once

#include <string>

namespace kuzu {
namespace main {
class Database;

class Extension {
public:
    virtual ~Extension() = default;

    //virtual void load(Database& db) = 0;
};

} // namespace main
} // namespace kuzu
