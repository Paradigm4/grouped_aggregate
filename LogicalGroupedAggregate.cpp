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
using grouped_aggregate::Settings;

class LogicalGroupedAggregate : public LogicalOperator
{
public:
    LogicalGroupedAggregate(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
        ADD_PARAM_VARIES()
        //TODO
        _usage = "write me a usage, bro!\n";
    }

    std::vector<std::shared_ptr<OperatorParamPlaceholder> >
    nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<std::shared_ptr<OperatorParamPlaceholder> > res;
        if(_parameters.size()>=2)
        {
            res.push_back(END_OF_VARIES_PARAMS());
        }
        res.push_back(PARAM_AGGREGATE_CALL());
        res.push_back(PARAM_IN_ATTRIBUTE_NAME("void"));
        res.push_back(PARAM_IN_DIMENSION_NAME());
        return res;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        size_t const numInstances = query->getInstancesCount();
        grouped_aggregate::Settings settings(schemas[0], _parameters, true, query);
        return settings.makeSchema(Settings::FINAL, schemas[0].getName());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalGroupedAggregate, "grouped_aggregate");

}
