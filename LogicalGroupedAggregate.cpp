/*
**
* BEGIN_COPYRIGHT
*
* PARADIGM4 INC.
* This file is part of the Paradigm4 Enterprise SciDB distribution kit
* and may only be used with a valid Paradigm4 contract and in accord
* with the terms and conditions specified by that contract.
*
* Copyright (C) 2010 - 2015 Paradigm4 Inc.
* All Rights Reserved.
*
* END_COPYRIGHT
*/

#include "query/Operator.h"

namespace scidb
{

using namespace std;

class LogicalGroupedAggregate : public LogicalOperator
{
public:
    LogicalGroupedAggregate(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        //TODO
        _usage = "write me a usage, bro!\n";
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        Attributes outputAttributes;
        outputAttributes.push_back( AttributeDesc(0, "num_chunks", TID_UINT64, 0, 0));
        outputAttributes.push_back( AttributeDesc(1, "num_cells", TID_UINT64, 0, 0));
        outputAttributes.push_back( AttributeDesc(2, "num_unique", TID_UINT64, 0, 0));
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("instance_no", 0, query->getInstancesCount(), 1, 0));
        return ArrayDesc("grouped_aggregte_test", outputAttributes, outputDimensions,  defaultPartitioning());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalGroupedAggregate, "grouped_aggregate");

}
