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
#include <sys/time.h>

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

typedef unsigned long long timestamp_t;

static timestamp_t get_timestamp ()
{
  struct timeval now;
  gettimeofday (&now, NULL);
  return  now.tv_usec + (timestamp_t)now.tv_sec * 1000000;
}



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

        OptimalBloom optbloom;

    	shared_ptr<Array>& input = inputArrays[0];
    	shared_ptr< Array> outArray;
    	shared_ptr<Array>& array = input;
    	std::shared_ptr<ConstArrayIterator> inputIterator = input->getConstIterator(1);

    	std::vector<double> pstore;

    	optbloom.setMaxBloomSize(50000e6);
        optbloom.setEstimatedElementCount(10);

        optbloom.setFalsePosProb(.000001);
        optbloom.computeOptimalParameters();
        pstore.push_back(optbloom.getPRealized());

		optbloom.setFalsePosProb(.0001);
    	optbloom.computeOptimalParameters();
    	optbloom.getPRealized();
    	pstore.push_back(optbloom.getPRealized());

    	optbloom.setFalsePosProb(.01);
    	optbloom.computeOptimalParameters();
    	pstore.push_back(optbloom.getPRealized());

    	optbloom.setFalsePosProb(.1);
    	optbloom.computeOptimalParameters();
    	pstore.push_back(optbloom.getPRealized());

    	optbloom.setFalsePosProb(0.5);
    	optbloom.computeOptimalParameters();
    	pstore.push_back(optbloom.getPRealized());

    	LOG4CXX_DEBUG(loggerP,"BLOOM computeOptimalParameters finished");
    	bloom::BloomFilter *foofilter = new BloomFilter(optbloom);
    	LOG4CXX_DEBUG(loggerP,"BLOOM filter allocated");
        size_t bloomEntries = 0;
        double addTime=0;
    	while(!inputIterator-> end())
        	{
        		std::shared_ptr<ConstChunkIterator> inputChunkIterator = inputIterator->getChunk().getConstIterator();
        		while(!inputChunkIterator->end())
        		{
        			Value foo = inputChunkIterator->getItem();

        			timestamp_t t0 = get_timestamp();
        			foofilter->addData((char*)foo.data(), foo.size());
        			timestamp_t t1 = get_timestamp();
        			double secs = (t1 - t0) / 1000000.0L;
					addTime+=secs;
        			bloomEntries++;
        			//LOG4CXX_DEBUG(loggerP,"size="<<foo.size()<< " ,DATA=" << myString);
        			++(*inputChunkIterator);
        		}
        		//ConstChunk const& chunk = inputIterator->getChunk();
        		++(*inputIterator);
        	}
    	Coordinates positions(1);
    	positions[0] = (Coordinate)1;
        inputIterator->setPosition(positions);

    	double checkTime = 0;
    	size_t wrong = 0;
    	size_t space = 0;
    	while(!inputIterator-> end())
    	{
    		std::shared_ptr<ConstChunkIterator> inputChunkIterator = inputIterator->getChunk().getConstIterator();
    		while(!inputChunkIterator->end())
    		{
    			Value foo = inputChunkIterator->getItem();

    			timestamp_t t0 = get_timestamp();
    			bool hasdata = foofilter->hasData((char*)foo.data(), foo.size());
    			timestamp_t t1 = get_timestamp();
    			space += foo.size();
    			if(!hasdata)
    				wrong+=1;
    			double secs = (t1 - t0) / 1000000.0L;
    			checkTime+=secs;
    			++(*inputChunkIterator);
    		}
    		++(*inputIterator);
    	}

        bool rtn = foofilter->hasData("abc",sizeof("abc"));
		LOG4CXX_DEBUG(loggerP,"BLOOM hasData=" << rtn);
		double aveinsert = addTime/(double)bloomEntries;
		double avecheck = checkTime/(double)bloomEntries;
		double avgsize = space/(double)bloomEntries;
		LOG4CXX_DEBUG(loggerP,"BLOOM RUNTIME number_inserts=" << bloomEntries << ", ave_insert_time=" << aveinsert << ", check_time=" << avecheck << ", avg_space_per_entry=" << avgsize );
		double wrongper = (double)wrong/(double)bloomEntries;
		//      ssdreadcost = 8MB disk write on SSD = 8/350 = 0.2285714285714 s
		//		8e6 bytes in one chunk / 4 bytes per string = 2e6 strings in a chunk
		//      9e-6 ave time for one insert into bloom
        //      estimated_element_write = 1e6;
		//      costBloom = 9e-6 (sec per 4 byte insert) * estimated_num_elements + number_element_check * 2e-6
		//      costDisk  = ceil((number_element_check * 4 bytes)%8e6)*8 / 350
		//      ROI = costDisk/CostBloom

		LOG4CXX_DEBUG(loggerP,"BLOOM RUNTIME number_inserts=" << bloomEntries << ", number_wrong=" << wrong << ", wrong_percent=" << wrongper << ", theory_p_false=" << optbloom.getPRealized() );
		return array;
    }
};
REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalBloom, "bloom", "physical_bloom");
} //namespace scidb
