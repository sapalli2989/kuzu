#include <iostream>

#include "kuzu.hpp"

#include <chrono>
using namespace kuzu::main;

int main() {
    auto database = std::make_unique<Database>("/Users/monkey/Desktop/博士/code/KUZU_PROJECT/kuzu/build/release/tools/shell/var_list_test_2" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());
//    auto result =connection->query("create node table person (ID INT64, age INT64[], PRIMARY KEY (ID));");
//    std::cout << result->toString();
    // Execute a simple query.
    auto start = std::chrono::steady_clock::now();
    auto result =connection->query("MATCH (a:person) WHERE a.ID=1000000 set a.age=[0,0,0] return a.age;");
    auto end = std::chrono::steady_clock::now();
    std::cout << result->toString();
    // 计算时间差并转换为毫秒
    auto diff = end - start;
    double elapsed_ms = std::chrono::duration<double, std::milli>(diff).count();
    // 打印执行时间
    std::cout << "Execution time: " << elapsed_ms << " ms" << std::endl;
    //    result = connection->query("MATCH (a:person) WHERE a.ID=100 set a.age=[0] return a.age;");
//    COPY person FROM '/Users/monkey/Desktop/博士/code/KUZU_PROJECT/var_list_test/person2.csv';
//    std::cout << result->toString();
}
