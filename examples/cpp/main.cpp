#include <iostream>

#include "kuzu.hpp"
using namespace kuzu::main;

void create(Connection* connection){
    std::cout << connection->query("CREATE NODE TABLE Person(ID INT64, age INT64, PRIMARY KEY(ID));")->toString();
    std::cout << connection->query("CREATE REL TABLE knows(FROM Person TO Person, age iNT64);")->toString();
    std::cout <<  connection->query("CREATE (:Person {ID:0, age: 25});")->toString();
    std::cout << connection->query("CREATE (:Person {ID:1, age: 30});")->toString();
    std::cout <<  connection->query("CREATE (:Person {ID:2, age: 36});")->toString();
    std::cout <<  connection->query("CREATE (:Person {ID:3, age: 36});")->toString();
    std::cout <<  connection->query("CREATE (:Person {ID:4, age: 36});")->toString();
    std::cout <<  connection->query("CREATE (:Person {ID:5, age: 36});")->toString();
    std::cout <<  connection->query("CREATE (:Person {ID:6, age: 36});")->toString();
    std::cout <<  connection->query("CREATE (:Person {ID:7, age: 36});")->toString();

    std::cout <<  connection->query("MATCH(p:Person {ID:0}),(p1:Person {ID:1}) CREATE (p)-[knows{age:2}]->(p1);")->toString();
    std::cout <<  connection->query("MATCH(p:Person {ID:1}),(p1:Person {ID:2}) CREATE (p)-[knows{age:3}]->(p1);")->toString();
    std::cout <<  connection->query("MATCH(p:Person {ID:2}),(p1:Person {ID:3}) CREATE (p)-[knows{age:4}]->(p1);")->toString();
    std::cout <<  connection->query("MATCH(p:Person {ID:3}),(p1:Person {ID:4}) CREATE (p)-[knows{age:5}]->(p1);")->toString();
    std::cout <<  connection->query("MATCH(p:Person {ID:4}),(p1:Person {ID:5}) CREATE (p)-[knows{age:6}]->(p1);")->toString();
    std::cout <<  connection->query("MATCH(p:Person {ID:5}),(p1:Person {ID:6}) CREATE (p)-[knows{age:7}]->(p1);")->toString();
    std::cout <<  connection->query("MATCH(p:Person {ID:6}),(p1:Person {ID:7}) CREATE (p)-[knows{age:8}]->(p1);")->toString();
}
int main() {
    SystemConfig systemConfig;
    systemConfig.maxNumThreads=10;
    auto database = std::make_unique<Database>("test" /* fill db path */,systemConfig);
    auto connection = std::make_unique<Connection>(database.get());

    // Create schema.
//    std::cout << connection->query("CREATE NODE TABLE Person(ID INT64, age INT64, PRIMARY KEY(ID));")->toString();
//     std::cout << connection->query("CREATE REL TABLE knows(FROM Person TO Person, ONE_ONE);")->toString();
////////    // Create nodes.
//   std::cout <<  connection->query("CREATE (:Person {ID:0, age: 25});")->toString();
//    std::cout << connection->query("CREATE (:Person {ID:1, age: 30});")->toString();
//   std::cout <<  connection->query("CREATE (:Person {ID:2, age: 36});")->toString();
//    std::cout <<  connection->query("CREATE (:Person {ID:3, age: 36});")->toString();
//    std::cout <<  connection->query("CREATE (:Person {ID:4, age: 36});")->toString();
//    std::cout <<  connection->query("CREATE (:Person {ID:5, age: 36});")->toString();
//   std::cout <<  connection->query("MATCH(p:Person {ID:0}),(p1:Person {ID:1}) CREATE (p)-[knows]->(p1);")->toString();
//////   std::cout <<  connection->query("MATCH(p:Person {ID:0}),(p1:Person {ID:2}) CREATE (p)-[knows]->(p1);")->toString();
//    std::cout <<  connection->query("MATCH(p:Person {ID:1}),(p1:Person {ID:2}) CREATE (p)-[knows]->(p1);")->toString();
//   std::cout <<  connection->query("MATCH(p:Person {ID:2}),(p1:Person {ID:3}) CREATE (p)-[knows]->(p1);")->toString();
//    std::cout <<  connection->query("MATCH(p:Person {ID:3}),(p1:Person {ID:4}) CREATE (p)-[knows]->(p1);")->toString();
//    std::cout <<  connection->query("MATCH(p:Person {ID:4}),(p1:Person {ID:5}) CREATE (p)-[knows]->(p1);")->toString();

//    std::cout <<  connection->query("MATCH(p:Person {ID:0}),(p1:Person {ID:3}) CREATE (p)-[knows]->(p1);")->toString();
// Execute a simple query.

//    create(connection.get());
    auto result = connection->query("MATCH (a)-[r]->(b) return r.age");
    // Print query result.
    std::cout << result->toString();
}
