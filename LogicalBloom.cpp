/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* bloom is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* bloom is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* bloom is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with bloom.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include "query/Operator.h"
#include "BloomSettings.h"

namespace scidb
{

using namespace std;
using bloom::Settings;

class LogicalBloom : public LogicalOperator
{
public:
    LogicalBloom(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
		ADD_PARAM_INPUT()
        //ADD_PARAM_VARIES()
        //TODO
        _usage = "write me a usage, bro!\n";
    }

    std::vector<std::shared_ptr<OperatorParamPlaceholder> >
    nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<std::shared_ptr<OperatorParamPlaceholder> > res;
        //if(_parameters.size()>=2)
        {
            res.push_back(END_OF_VARIES_PARAMS());
        }
        //res.push_back(PARAM_CONSTANT("string"));
        return res;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
    	vector<ArrayDesc const*> inputSchemas;
    	inputSchemas.push_back(&(schemas[0]));
    	inputSchemas.push_back(&(schemas[1]));
    	Settings settings(inputSchemas, _parameters, true, query);
        return settings.makeSchema(query, Settings::FINAL, schemas[0].getName());
    }

};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalBloom, "bloom");

}
