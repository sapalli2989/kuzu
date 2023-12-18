#include <iostream>
#include <random>

#include "kuzu.hpp"

#define LOGGING true

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

static uint64_t getNumNodes(Connection* conn) {
    auto result = conn->query("MATCH (n:N) RETURN COUNT(*);");
    std::cout << result->toString() << std::endl;
    result->resetIterator();
    return result->getNext()->getValue(0)->val.int64Val;
}

void defineSchema(Connection* conn) {
    auto res =
        conn->query("CREATE NODE TABLE N(id SERIAL, name STRING, age INT64, PRIMARY KEY(id));");
    std::cout << res->toString() << std::endl;
    res = conn->query("CREATE REL TABLE R(FROM N TO N, strProp STRING, intProp INT64)");
    std::cout << res->toString() << std::endl;
}

uint64_t createNodes(Connection* conn, uint64_t counterInMS) {
    auto preparedStatement = conn->prepare("CREATE (n:N {name: $name, age: $age});");
    uint64_t numNodes = 0;
    kuzu::common::Timer timer;
    timer.start();
    while (timer.getElapsedTimeInMS() <= counterInMS) {
        auto strLen = randInt64(10, 100);
        auto strProp = random_string(strLen);
#if LOGGING
        std::cout << "strProp: " << strProp << std::endl;
#endif
        auto res = conn->execute(preparedStatement.get(),
            std::make_pair<std::string, const char*>("name", strProp.c_str()),
            std::make_pair<std::string, int64_t>("age", randInt64()));
#if LOGGING
        std::cout << res->toString() << std::endl;
#endif
        numNodes++;
    }
    auto elapsed = timer.getElapsedTimeInMS();
    std::cout << numNodes << " ops within time: " << elapsed << " ms" << std::endl;
    std::cout << "Throughput: " << numNodes / (elapsed / 1000.0) << " ops" << std::endl;
    return numNodes;
}

uint64_t createRels(Connection* conn, uint64_t counterInMS) {
    auto numNodes = getNumNodes(conn);
    auto preparedStatement = conn->prepare(
        "MATCH (n1:N), (n2:N) WHERE n1.id=$n1 AND n2.id=$n2 CREATE (n1)-[r:R {strProp: $strProp, "
        "intProp: $intProp}]->(n2);");
    uint64_t numRels = 0;
    kuzu::common::Timer timer;
    timer.start();
    while (timer.getElapsedTimeInMS() <= counterInMS) {
        int64_t srcNodeID = randInt64(0, numNodes - 1);
        int64_t dstNodeID = randInt64(0, numNodes - 1);
        auto strLen = randInt64(10, 100);
        auto strProp = random_string(strLen);
        int64_t intProp = randInt64();
#if LOGGING
        std::cout << "srcNodeID: " << srcNodeID << ", dstNodeID: " << dstNodeID
                  << ", strProp: " << strProp << ", intProp: " << intProp << std::endl;
#endif
        auto res = conn->execute(preparedStatement.get(),
            std::make_pair<std::string, int64_t>("n1", std::move(srcNodeID)),
            std::make_pair<std::string, int64_t>("n2", std::move(dstNodeID)),
            std::make_pair<std::string, const char*>("strProp", strProp.c_str()),
            std::make_pair<std::string, int64_t>("intProp", std::move(intProp)));
#if LOGGING
        std::cout << res->toString() << std::endl;
#endif
        numRels++;
    }
    auto elapsed = timer.getElapsedTimeInMS();
    std::cout << numRels << " ops within time: " << elapsed << " ms" << std::endl;
    std::cout << "Throughput: " << numRels / (elapsed / 1000.0) << " ops" << std::endl;
    return numRels;
}

void updateNodesIntProp(Connection* conn, uint64_t counterInMS) {
    auto numNodes = getNumNodes(conn);
    auto preparedStatement = conn->prepare("MATCH (n:N) WHERE n.id=$id SET n.age = $age;");
    uint64_t numOps = 0;
    kuzu::common::Timer timer;
    timer.start();
    while (timer.getElapsedTimeInMS() <= counterInMS) {
        auto nodeID = randInt64(0, numNodes - 1);
        auto age = randInt64();
#if LOGGING
        std::cout << "nodeID: " << nodeID << ", age: " << age << std::endl;
#endif
        auto res = conn->execute(preparedStatement.get(),
            std::make_pair<std::string, int64_t>("id", std::move(nodeID)),
            std::make_pair<std::string, int64_t>("age", std::move(age)));
#if LOGGING
        std::cout << res->toString() << std::endl;
#endif
        numOps++;
    }
    auto elapsed = timer.getElapsedTimeInMS();
    std::cout << numOps << " ops within time: " << elapsed << " ms" << std::endl;
    std::cout << "Throughput: " << numOps / (elapsed / 1000.0) << " ops" << std::endl;
}

void updateRelIntProp(Connection* conn, uint64_t counterInMS) {
    auto numNodes = getNumNodes(conn);
    auto preparedStatement = conn->prepare(
        "MATCH (n1:N)-[r:R]->(n2:N) WHERE n1.id=$n1 AND n2.id=$n2 SET r.intProp = $intProp;");
    uint64_t numOps = 0;
    kuzu::common::Timer timer;
    timer.start();
    while (timer.getElapsedTimeInMS() <= counterInMS) {
        int64_t srcNodeID = randInt64(0, numNodes - 1);
        int64_t dstNodeID = randInt64(0, numNodes - 1);
        int64_t intProp = randInt64();
#if LOGGING
        std::cout << "srcNodeID: " << srcNodeID << ", dstNodeID: " << dstNodeID
                  << ", intProp: " << intProp << std::endl;
#endif
        auto res = conn->execute(preparedStatement.get(),
            std::make_pair<std::string, int64_t>("n1", std::move(srcNodeID)),
            std::make_pair<std::string, int64_t>("n2", std::move(dstNodeID)),
            std::make_pair<std::string, int64_t>("intProp", std::move(intProp)));
#if LOGGING
        std::cout << res->toString() << std::endl;
#endif
        numOps++;
    }
    auto elapsed = timer.getElapsedTimeInMS();
    std::cout << numOps << " ops within time: " << elapsed << " ms" << std::endl;
    std::cout << "Throughput: " << numOps / (elapsed / 1000.0) << " ops" << std::endl;
}

void deleteNodes(Connection* conn, uint64_t counterInMS) {
    auto numNodes = getNumNodes(conn);
    auto preparedStatement = conn->prepare("MATCH (n:N) WHERE n.id=$id DELETE n;");
    uint64_t numOps = 0;
    kuzu::common::Timer timer;
    timer.start();
    while (timer.getElapsedTimeInMS() <= counterInMS) {
        auto nodeID = randInt64(0, numNodes - 1);
#if LOGGING
        std::cout << "nodeID: " << nodeID << std::endl;
#endif
        auto res = conn->execute(
            preparedStatement.get(), std::make_pair<std::string, int64_t>("id", std::move(nodeID)));
#if LOGGING
        std::cout << res->toString() << std::endl;
#endif
        numOps++;
    }
    auto elapsed = timer.getElapsedTimeInMS();
    std::cout << numOps << " ops within time: " << elapsed << " ms" << std::endl;
    std::cout << "Throughput: " << numOps / (elapsed / 1000.0) << " ops" << std::endl;
}

void deleteRels(Connection* conn, uint64_t counterInMS) {
    auto numNodes = getNumNodes(conn);
    auto preparedStatement = conn->prepare("MATCH (n1:N)-[r:R]->(n2:N) WHERE n1.id=$n1 DELETE r;");
    uint64_t numOps = 0;
    kuzu::common::Timer timer;
    timer.start();
    while (timer.getElapsedTimeInMS() <= counterInMS) {
        auto srcNodeID = randInt64(0, numNodes - 1);
#if LOGGING
        std::cout << "srcNodeID: " << srcNodeID << std::endl;
#endif
        auto res = conn->execute(preparedStatement.get(),
            std::make_pair<std::string, int64_t>("n1", std::move(srcNodeID)));
#if LOGGING
        std::cout << res->toString() << std::endl;
#endif
        numOps++;
    }
    auto elapsed = timer.getElapsedTimeInMS();
    std::cout << numOps << " ops within time: " << elapsed << " ms" << std::endl;
    std::cout << "Throughput: " << numOps / (elapsed / 1000.0) << " ops" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cout << "Usage: " << argv[0] << " <db_path> <type> <time_in_seconds>" << std::endl;
        std::cout
            << "<type>: create_nodes, create_rels, set_nodes, set_rels, delete_nodes, delete_rels"
            << std::endl;
        return 1;
    }
    SystemConfig systemConfig;
    auto dbPath = argv[1];
    auto type = std::string(argv[2]);
    auto timeInSeconds = std::stoi(argv[3]);
    std::cout << "Start at db path: " << dbPath << ", type: " << type
              << ", seconds: " << timeInSeconds << std::endl;
    auto timeInMS = timeInSeconds * 1000;
    auto database = std::make_unique<Database>(dbPath, systemConfig);
    auto connection = std::make_unique<Connection>(database.get());
    // Create schema.
    defineSchema(connection.get());
    if (type == "create_nodes") {
        createNodes(connection.get(), timeInMS);
    } else if (type == "create_rels") {
        createRels(connection.get(), timeInMS);
    } else if (type == "set_nodes") {
        updateNodesIntProp(connection.get(), timeInMS);
    } else if (type == "set_rels") {
        updateRelIntProp(connection.get(), timeInMS);
    } else if (type == "delete_nodes") {
        deleteNodes(connection.get(), timeInMS);
    } else if (type == "delete_rels") {
        deleteRels(connection.get(), timeInMS);
    } else {
        std::cout << "Unknown type: " << type << std::endl;
        return 1;
    }
}
