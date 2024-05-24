#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("testdb" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());
    std::cout << connection->query("call show_connection(\"studyAt\") return *;")->toString()
              << std::endl;
}
