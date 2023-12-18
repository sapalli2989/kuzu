#pragma once

#include "main/connection.h"
#include "test_runner/test_group.h"

namespace kuzu {
namespace testing {

class TestRunner {
public:
    static void runTest(
        TestStatement* statement, main::Connection& conn, std::string& databasePath);

    static std::unique_ptr<planner::LogicalPlan> getLogicalPlan(
        const std::string& query, main::Connection& conn);

private:
    static void testStatement(
        TestStatement* statement, main::Connection& conn, std::string& databasePath);
    static void checkLogicalPlans(std::unique_ptr<main::PreparedStatement>& preparedStatement,
        TestStatement* statement, main::Connection& conn);
    static void checkLogicalPlan(std::unique_ptr<main::PreparedStatement>& preparedStatement,
        TestStatement* statement, main::Connection& conn, uint32_t planIdx);
    static std::vector<std::string> convertResultToString(
        main::QueryResult& queryResult, bool checkOutputOrder = false);
    static std::string convertResultToMD5Hash(
        main::QueryResult& queryResult); // returns hash and number of values hashed
    static void checkPlanResult(std::unique_ptr<main::QueryResult>& result,
        TestStatement* statement, const std::string& planStr, uint32_t planIndex);
};

} // namespace testing
} // namespace kuzu
