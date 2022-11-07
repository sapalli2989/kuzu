#pragma once

#include "src/catalog/include/catalog.h"
#include "src/processor/operator/include/physical_operator.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::catalog;

namespace graphflow {
namespace processor {

class DDL : public PhysicalOperator {

public:
    DDL(Catalog* catalog, uint32_t id, const string& paramsString)
        : PhysicalOperator{id, paramsString}, catalog{catalog} {}

    virtual string execute() = 0;

    bool getNextTuples() override { assert(false); }

    virtual ~DDL() = default;

protected:
    Catalog* catalog;
};

} // namespace processor
} // namespace graphflow