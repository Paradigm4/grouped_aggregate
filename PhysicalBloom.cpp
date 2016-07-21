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


#include <query/Operator.h>
#include <array/Metadata.h>
#include <system/Cluster.h>
#include <query/Query.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <log4cxx/logger.h>
#include <util/NetworkMessage.h>
#include <array/RLE.h>
#include <array/SortArray.h>

#include "query/Operator.h"
#include <array/SortArray.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory>
#include <cstddef>

#include "BloomUtilities.h"
#include "BloomSettings.h"

using namespace boost;
using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr loggerP(log4cxx::Logger::getLogger("scidb.operators.bloom.physical"));
using bloom::Settings;

namespace bloom
{

} //namespace bloom

using namespace bloom;

class PhysicalBloom : public PhysicalOperator
{
	 typedef map<Coordinate, Value> CoordValueMap;
	 typedef std::pair<Coordinate, Value> CoordValueMapEntry;

public:
    PhysicalBloom(string const& logicalName,
                             string const& physicalName,
                             Parameters const& parameters,
                             ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual RedistributeContext getOutputDistribution(
               std::vector<RedistributeContext> const& inputDistributions,
               std::vector< ArrayDesc> const& inputSchemas) const
    {
        return RedistributeContext(createDistribution(psUndefined), _schema.getResidency() );
    }

    /*shared_ptr<Array> flatSort(shared_ptr<Array> & input, shared_ptr<Query>& query, Settings& settings)
    {
        SortingAttributeInfos sortingAttributeInfos(settings.getGroupSize() + 1);
        sortingAttributeInfos[0].columnNo = 0;
        sortingAttributeInfos[0].ascent = true;
        for(size_t g=0; g<settings.getGroupSize(); ++g)
        {
            sortingAttributeInfos[g+1].columnNo = g+1;
            sortingAttributeInfos[g+1].ascent = true;
        }
        SortArray sorter(input->getArrayDesc(), _arena, false, settings.getSpilloverChunkSize());
        shared_ptr<TupleComparator> tcomp(make_shared<TupleComparator>(sortingAttributeInfos, input->getArrayDesc()));
        return sorter.getSortedArray(input, query, tcomp);
    }
*/
    /*shared_ptr<Array> localCondense(shared_ptr<Array>& inputArray, shared_ptr<Query>& query, Settings& settings)
    {
        ArenaPtr operatorArena = this->getArena();
        ArenaPtr hashArena(newArena(Options("").resetting(true).threading(false).pagesize(8 * 1024 * 1204).parent(operatorArena)));
        AggregateHashTable aht(settings, hashArena);

        return .finalize();
    }
    */

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {

    	//vector<ArrayDesc const*> inputSchemas;
    	//inputSchemas.push_back(&(schemas[0]));
    	//inputSchemas.push_back(&(schemas[1]));`
    	//Settings settings(inputArrays[0]->getArrayDesc(), _parameters, false, query);
        bloom::BloomFilter foofilter;


        //bloom::BloomFilter foofilter;
    	/*
		 minBloomSize(1),
		 maxBloomSize(std::numeric_limits<unsigned long long int>::max()),
		 minNumHashes(1),
		 maxNumHashes(std::numeric_limits<unsigned int>::max()),
		 estimatedElementCount(100000),
		 falsePosProb(1.0 / estimatedElementCount
        */


    	shared_ptr<Array>& input = inputArrays[0];
    	shared_ptr< Array> outArray;
    	shared_ptr<Array>& array = input;
    	std::shared_ptr<ConstArrayIterator> inputIterator = input->getConstIterator(1);

    	foofilter.setMaxBloomSize(50000e6);
        foofilter.setEstimatedElementCount(1000000000);
    	foofilter.setFalsePosProb(.000001);

        foofilter.computeOptimalParameters();

    	foofilter.setFalsePosProb(.0001);
    	foofilter.computeOptimalParameters();

    	foofilter.setFalsePosProb(.01);
    	foofilter.computeOptimalParameters();

    	foofilter.setFalsePosProb(.1);
    	foofilter.computeOptimalParameters();

    	foofilter.setFalsePosProb(.5);
    	foofilter.computeOptimalParameters();


        while(!inputIterator-> end())
        	{
        		std::shared_ptr<ConstChunkIterator> inputChunkIterator = inputIterator->getChunk().getConstIterator();
        		while(!inputChunkIterator->end())
        		{
        			Value foo = inputChunkIterator->getItem();
        			//void addData( char const* data, size_t const dataSize )
        			//bool hasData(char const* data, size_t const dataSize ) const
        			//foofilter->addData(foo.getString(), foo.size());
        			foofilter.addData((char*)foo.data(), foo.size());
        			//std::string myString;
        			//myString.assign((char*)foo.data(), foo.size());
        			//LOG4CXX_DEBUG(loggerP,"size="<<foo.size()<< " ,DATA=" << myString);
        			//foofilter.addData("abc", sizeof("abc"));
        			++(*inputChunkIterator);
        		}
        		//ConstChunk const& chunk = inputIterator->getChunk();
        		++(*inputIterator);
        	}

        bool rtn = foofilter.hasData("abc",sizeof("abc"));
		LOG4CXX_DEBUG(loggerP,"BLOOM hasData=" << rtn);

        /*
        array = localCondense(array, query, settings);
        array = globalMerge(array, query, settings);
        */
        return array;
    }
};
REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalBloom, "bloom", "physical_bloom");
} //namespace scidb
