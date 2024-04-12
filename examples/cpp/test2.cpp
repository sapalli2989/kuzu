#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("test" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());

    // Create schema.
    //    connection->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));");
    // Create nodes.
    //    auto result=connection->query("create macro addWithDefault(a,b:=3) as a + b;");
    //    std::cout << result->toString();
    auto result=connection->query("COMMENT ON MACRO addWithDefault is '1 good macro'");
    std::cout << result->toString();
    //    connection->query("CREATE (:Person {name: 'Bob', age: 30});");
    //
    //    // Execute a simple query.
    //    auto result = connection->query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;");
    // Print query result.
    result=connection->query("CALL SHOW_TABLES() RETURN *;");
    std::cout << result->toString();
}
