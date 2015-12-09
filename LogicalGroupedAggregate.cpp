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
#include "GroupedAggregateSettings.h"

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
        ADD_PARAM_IN_ATTRIBUTE_NAME("void")
        ADD_PARAM_AGGREGATE_CALL()
        //TODO
        _usage = "write me a usage, bro!\n";
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        size_t const numInstances = query->getInstancesCount();
        grouped_aggregate::Settings settings(schemas[0], _parameters, true, query);
        Attributes outputAttributes;
        outputAttributes.push_back( AttributeDesc(0, "hash",   TID_UINT64,    0, 0));
        outputAttributes.push_back( AttributeDesc(1, "group",  settings.getGroupAttributeType(), 0, 0));
        outputAttributes.push_back( AttributeDesc(2, "state",  settings.getStateType(), AttributeDesc::IS_NULLABLE, 0));
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("dst_instance_id", 0, numInstances-1, 1, 0));
        outputDimensions.push_back(DimensionDesc("src_instance_id", 0, numInstances-1, 1, 0));
        outputDimensions.push_back(DimensionDesc("block_no",        0, CoordinateBounds::getMax(), 1, 0));
        outputDimensions.push_back(DimensionDesc("value_no",        0, 1000000-1, 1000000, 0));
        return ArrayDesc("grouped_aggregate_state", outputAttributes, outputDimensions, defaultPartitioning());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalGroupedAggregate, "grouped_aggregate");

}
