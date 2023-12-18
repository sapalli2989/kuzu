#include "test_runner/test_runner.h"

#include <fstream>

#include "common/exception/test.h"
#include "common/md5.h"
#include "common/string_utils.h"
#include "gtest/gtest.h"
#include "planner/operator/logical_plan.h"
#include "spdlog/spdlog.h"
#include "test_helper/test_helper.h"

using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace testing {

void TestRunner::runTest(TestStatement* statement, Connection& conn, std::string& databasePath) {
    // for batch statements
    if (!statement->batchStatmentsCSVFile.empty()) {
        TestHelper::executeScript(statement->batchStatmentsCSVFile, conn);
        return;
    }
    // for normal statement
    spdlog::info("DEBUG LOG: {}", statement->logMessage);
    spdlog::info("QUERY: {}", statement->query);
    conn.setMaxNumThreadForExec(statement->numThreads);
    testStatement(statement, conn, databasePath);
}

void TestRunner::testStatement(
    TestStatement* statement, Connection& conn, std::string& databasePath) {
    std::unique_ptr<PreparedStatement> preparedStatement;
    StringUtils::replaceAll(statement->query, "${DATABASE_PATH}", databasePath);
    StringUtils::replaceAll(statement->query, "${KUZU_ROOT_DIRECTORY}", KUZU_ROOT_DIRECTORY);
    if (statement->encodedJoin.empty()) {
        preparedStatement = conn.prepareNoLock(statement->query, statement->enumerate);
    } else {
        preparedStatement = conn.prepareNoLock(statement->query, true, statement->encodedJoin);
    }
    // Check for wrong statements
    if (!statement->expectedError) {
        ASSERT_TRUE(preparedStatement->isSuccess()) << preparedStatement->getErrorMessage();
    }
    checkLogicalPlans(preparedStatement, statement, conn);
}

void TestRunner::checkLogicalPlans(std::unique_ptr<PreparedStatement>& preparedStatement,
    TestStatement* statement, Connection& conn) {
    auto numPlans = preparedStatement->logicalPlans.size();
    auto numPassedPlans = 0u;
    if (numPlans == 0) {
        checkLogicalPlan(preparedStatement, statement, conn, 0);
    }
    for (auto i = 0u; i < numPlans; ++i) {
        checkLogicalPlan(preparedStatement, statement, conn, i);
    }
}

void TestRunner::checkLogicalPlan(std::unique_ptr<PreparedStatement>& preparedStatement,
    TestStatement* statement, Connection& conn, uint32_t planIdx) {
    auto result = conn.executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get(), planIdx);
    if (statement->expectedError) {
        EXPECT_FALSE(result->isSuccess());
        ASSERT_EQ(statement->errorMessage, StringUtils::rtrim(result->getErrorMessage()));
    } else if (statement->expectedOk && result->isSuccess()) {
        return;
    } else {
        auto planStr = preparedStatement->logicalPlans[planIdx]->toString();
        checkPlanResult(result, statement, planStr, planIdx);
    }
}

void TestRunner::checkPlanResult(std::unique_ptr<QueryResult>& result, TestStatement* statement,
    const std::string& planStr, uint32_t planIdx) {

    if (!statement->expectedTuplesCSVFile.empty()) {
        std::ifstream expectedTuplesFile(statement->expectedTuplesCSVFile);
        if (!expectedTuplesFile.is_open()) {
            throw TestException("Cannot open file: " + statement->expectedTuplesCSVFile);
        }
        std::string line;
        while (std::getline(expectedTuplesFile, line)) {
            statement->expectedTuples.push_back(line);
        }
        if (!statement->checkOutputOrder) {
            sort(statement->expectedTuples.begin(), statement->expectedTuples.end());
        }
    }
    std::vector<std::string> resultTuples =
        TestRunner::convertResultToString(*result, statement->checkOutputOrder);
    if (statement->expectHash) {
        std::string resultHash = TestRunner::convertResultToMD5Hash(*result);
        if (resultTuples.size() == result->getNumTuples() &&
            resultHash == statement->expectedHashValue &&
            resultTuples.size() == statement->expectedNumTuples) {
            spdlog::info(
                "PLAN{} PASSED in {}ms.", planIdx, result->getQuerySummary()->getExecutionTime());
        } else {
            spdlog::error("PLAN{} NOT PASSED.", planIdx);
            spdlog::info("PLAN: \n{}", planStr);
            spdlog::info("RESULT: \n");
            for (auto& tuple : resultTuples) {
                spdlog::info(tuple);
            }
            ASSERT_EQ(resultHash, statement->expectedHashValue);
        }
    }
    if (resultTuples.size() == result->getNumTuples() &&
        resultTuples == statement->expectedTuples) {
        spdlog::info(
            "PLAN{} PASSED in {}ms.", planIdx, result->getQuerySummary()->getExecutionTime());
        return;
    } else {
        spdlog::error("PLAN{} NOT PASSED.", planIdx);
        spdlog::info("PLAN: \n{}", planStr);
        spdlog::info("RESULT: \n");
        if (resultTuples.size() != statement->expectedTuples.size()) {
            EXPECT_EQ(resultTuples.size(), statement->expectedTuples.size());
            ASSERT_EQ(resultTuples, statement->expectedTuples);
        } else {
            for (auto i = 0u; i < resultTuples.size(); i++) {
                EXPECT_EQ(resultTuples[i], statement->expectedTuples[i])
                    << "Result tuple at index " << i << " did not match the expected value";
            }
        }
    }
}

std::vector<std::string> TestRunner::convertResultToString(
    QueryResult& queryResult, bool checkOutputOrder) {
    std::vector<std::string> actualOutput;
    while (queryResult.hasNext()) {
        auto tuple = queryResult.getNext();
        actualOutput.push_back(tuple->toString(std::vector<uint32_t>(tuple->len(), 0)));
    }
    if (!checkOutputOrder) {
        sort(actualOutput.begin(), actualOutput.end());
    }
    return actualOutput;
}

std::string TestRunner::convertResultToMD5Hash(QueryResult& queryResult) {
    queryResult.resetIterator();
    MD5 hasher;
    while (queryResult.hasNext()) {
        const auto tuple = queryResult.getNext();
        for (uint32_t i = 0; i < tuple->len(); i++) {
            const auto val = tuple->getValue(i);
            if (val->isNull()) {
                hasher.addToMD5("NULL\n");
            } else {
                hasher.addToMD5(val->toString().c_str());
                hasher.addToMD5("\n");
            }
        }
    }
    return std::string(hasher.finishMD5());
}

std::unique_ptr<planner::LogicalPlan> TestRunner::getLogicalPlan(
    const std::string& query, kuzu::main::Connection& conn) {
    return std::move(conn.prepare(query)->logicalPlans[0]);
}

} // namespace testing
} // namespace kuzu
