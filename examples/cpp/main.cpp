#include <iostream>
#include <random>

#include "kuzu.hpp"

using namespace kuzu::main;

static auto& chrs = "0123456789"
                    "abcdefghijklmnopqrstuvwxyz"
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

static std::mt19937 rg{std::random_device{}()};
static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);

static std::string random_string(std::string::size_type length) {
    std::string s;
    s.reserve(length);
    while (length--)
        s += chrs[pick(rg)];
    return s;
}

static int64_t randInt64(int64_t min = 0, int64_t max = INT64_MAX) {
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(min, max);
    return dist(rng);
}

void defineSchema(Connection* conn) {
    auto res =
        conn->query("CREATE NODE TABLE N(id SERIAL, name STRING, age INT64, PRIMARY KEY(id));");
    std::cout << res->toString() << std::endl;
    res = conn->query("CREATE REL TABLE R(FROM N TO N, strProp STRING, intProp INT64)");
    std::cout << res->toString() << std::endl;
}

void createNodes(Connection* conn, int64_t numNodes) {
    auto preparedStatement = conn->prepare("CREATE (n:N {name: $name, age: $age});");
    kuzu::common::Timer timer;
    timer.start();
    for (auto i = 0; i < numNodes; i++) {
        auto strLen = randInt64(10, 100);
        auto strProp = random_string(strLen);
        auto intProp = randInt64();
        std::unordered_map<std::string, std::unique_ptr<kuzu::common::Value>> params;
        params["name"] = std::make_unique<kuzu::common::Value>(strProp.c_str());
        params["age"] = std::make_unique<kuzu::common::Value>(intProp);
        conn->executeWithParams(preparedStatement.get(), std::move(params));
        //        conn->execute(preparedStatement.get(),
        //            std::make_pair<std::string, std::string>("name", random_string(strLen)),
        //            std::make_pair<std::string, int64_t>("age", randInt64()));
    }
    auto elapsed = timer.getElapsedTimeInMS();
    std::cout << "Create nodes elapsed time: " << elapsed << " ms" << std::endl;
}

void createRels(Connection* conn, int64_t numNodes, int64_t numRels) {
    auto preparedStatement = conn->prepare(
        "MATCH (n1:N), (n2:N) WHERE n1.id=$n1 AND n2.id=$n2 CREATE (n1)-[r:R {strProp: $strProp, "
        "intProp: $intProp}]->(n2);");
    auto avgDegree = numRels / numNodes;
    kuzu::common::Timer timer;
    timer.start();
    for (auto i = 0; i < numNodes; i++) {
        for (auto j = 0; j < avgDegree; j++) {
            int64_t dstNodeID = randInt64(0, numNodes - 1);
            auto strLen = randInt64(10, 100);
            auto strProp = random_string(strLen);
            auto intProp = randInt64();
//            std::cout << "Create rel: " << i << " -> " << dstNodeID << ", strProp: " << strProp
//                      << ", intProp: " << intProp << std::endl;
            std::unordered_map<std::string, std::unique_ptr<kuzu::common::Value>> params;
            params["n1"] = std::make_unique<kuzu::common::Value>((int64_t)i);
            params["n2"] = std::make_unique<kuzu::common::Value>((int64_t)dstNodeID);
            params["strProp"] = std::make_unique<kuzu::common::Value>(strProp.c_str());
            params["intProp"] = std::make_unique<kuzu::common::Value>(intProp);
            conn->executeWithParams(preparedStatement.get(), std::move(params));
        }
    }
    auto elapsed = timer.getElapsedTimeInMS();
    std::cout << "Create rels elapsed time: " << elapsed << " ms" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cout << "Usage: " << argv[0] << " <db_path> <num_nodes> <num_rels>" << std::endl;
        return 1;
    }
    SystemConfig systemConfig;
    auto dbPath = argv[1];
    auto numNodes = std::stoi(argv[2]);
    auto numRels = std::stoi(argv[3]);
    std::cout << "Start at db path: " << dbPath << ", num nodes: " << numNodes
              << ", num rels: " << numRels << std::endl;
    auto database = std::make_unique<Database>(dbPath, systemConfig);
    auto connection = std::make_unique<Connection>(database.get());
    // Create schema.
    defineSchema(connection.get());
    // Create nodes.
    createNodes(connection.get(), numNodes);
    createRels(connection.get(), numNodes, numRels);

    auto result = connection->query("MATCH (n:N) RETURN COUNT(*);");
    std::cout << result->toString() << std::endl;
    result = connection->query("MATCH (n:N)-[r:R]->(m:N) RETURN COUNT(*);");
    std::cout << result->toString() << std::endl;
}
