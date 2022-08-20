#pragma once

#include "src/parser/ddl/include/ddl.h"

namespace graphflow {
namespace parser {

using namespace std;

class CreateNodeClause : public DDL {
public:
    explicit CreateNodeClause(
        string labelName, vector<pair<string, string>> propertyNameDataTypes, string primaryKey)
        : DDL{StatementType::CREATE_NODE_CLAUSE, move(labelName), move(propertyNameDataTypes)},
          primaryKey{move(primaryKey)} {}

    inline string getPrimaryKey() const { return primaryKey; }

private:
    string primaryKey;
};

} // namespace parser
} // namespace graphflow