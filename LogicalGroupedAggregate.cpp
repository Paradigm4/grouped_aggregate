/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* grouped_aggregate is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* grouped_aggregate is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* grouped_aggregate is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with grouped_aggregate.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/
#define LEGACY_API

#include "query/LogicalOperator.h"
#include "GroupedAggregateSettings.h"

namespace scidb
{

using namespace std;
using namespace grouped_aggregate;
using grouped_aggregate::Settings;

class LogicalGroupedAggregate : public LogicalOperator
{
public:
    LogicalGroupedAggregate(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        //TODO
        _usage = "write me a usage, bro!\n";
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::LIST, {
                 RE(PP(PLACEHOLDER_INPUT)),
                    RE(RE::LIST, {
                       RE(RE::PLUS, {
                          RE(RE::OR, {
                             RE(PP(PLACEHOLDER_AGGREGATE_CALL)),
                             RE(PP(PLACEHOLDER_ATTRIBUTE_NAME)),
                             RE(PP(PLACEHOLDER_DIMENSION_NAME))
                          }),
                       }),
                       RE(RE::PLUS, {
                          RE(RE::OR, {
                             RE(PP(PLACEHOLDER_AGGREGATE_CALL)),
                             RE(PP(PLACEHOLDER_ATTRIBUTE_NAME)),
                             RE(PP(PLACEHOLDER_DIMENSION_NAME))
                          }),
                       })
                 })
              })
            },
            { KW_INPUT_SORTED, RE(PP(PLACEHOLDER_CONSTANT, TID_BOOL)) },
            { KW_MAX_TABLE_SZ, RE(PP(PLACEHOLDER_CONSTANT, TID_UINT64)) },
            { KW_NUM_HASH_BCKTS, RE(PP(PLACEHOLDER_CONSTANT, TID_UINT64)) },
            { KW_SPILL_CHUNK_SZ, RE(PP(PLACEHOLDER_CONSTANT, TID_UINT64)) },
            { KW_MERGE_CHUNK_SZ, RE(PP(PLACEHOLDER_CONSTANT, TID_UINT64)) },
            { KW_OUTPUT_CHUNK_SZ, RE(PP(PLACEHOLDER_CONSTANT, TID_UINT64)) }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        size_t const numInstances = query->getInstancesCount();
        grouped_aggregate::Settings settings(schemas[0], _parameters, _kwParameters, query);
        return settings.makeSchema(query, Settings::FINAL, schemas[0].getName());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalGroupedAggregate, "grouped_aggregate");

}
