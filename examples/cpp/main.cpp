#include <iostream>

#include "kuzu.hpp"

using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("/Users/guodong/Downloads/d" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    auto r = connection->query("BEGIN TRANSACTION;");
    std::cout << r->toString() << std::endl;
    // Create schema.
    r = connection->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));");
    std::cout << r->toString() << std::endl;
    // Create nodes.
    r = connection->query("CREATE (:Person {name: 'Alice', age: 25});");
    std::cout << r->toString() << std::endl;
    r = connection->query("CREATE (:Person {name: 'Bob', age: 30});");
    std::cout << r->toString() << std::endl;
    // Execute a simple query.
    auto result = connection->query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;");
    // Print query result.
    std::cout << result->toString();
    r = connection->query("COMMIT;");
    std::cout << r->toString() << std::endl;
}
