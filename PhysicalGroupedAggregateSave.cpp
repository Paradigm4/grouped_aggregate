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


#include <query/Operator.h>
#include <array/Metadata.h>
#include <system/Cluster.h>
#include <query/Query.h>
#include <boost/make_shared.hpp>
#include <boost/foreach.hpp>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <log4cxx/logger.h>
#include <util/NetworkMessage.h>
#include <array/RLE.h>
#include <array/SortArray.h>

using namespace boost;
using namespace std;

#include "query/Operator.h"
#include "HashTableUtilities.h"
#include <array/SortArray.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

namespace scidb
{

class PhysicalGroupedAggregate : public PhysicalOperator
{

	 typedef map<Coordinate, Value> CoordValueMap;
	 typedef std::pair<Coordinate, Value> CoordValueMapEntry;

public:
    PhysicalGroupedAggregate(string const& logicalName,
                             string const& physicalName,
                             Parameters const& parameters,
                             ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual ArrayDistribution getOutputDistribution(vector<ArrayDistribution> const& inputDistributions,
                                                    vector<ArrayDesc> const& inputSchemas) const
    {
       return ArrayDistribution(psUndefined);
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const& sourceSchemas) const
    {
        return true;
    }

    Value& genRandomValue(TypeId const& type, Value& value, int percentNull, int nullReason)
          {
              assert(percentNull>=0 && percentNull<=100);

              if (percentNull>0 && rand()%100<percentNull) {
                  value.setNull(nullReason);
              } else if (type==TID_INT64) {
                  value.setInt64(rand());
              } else if (type==TID_BOOL) {
                  value.setBool(rand()%100<50);
              } else if (type==TID_STRING) {
                  vector<char> str;
                  const size_t maxLength = 300;
                  const size_t minLength = 1;
                  assert(minLength>0);
                  size_t length = rand()%(maxLength-minLength) + minLength;
                  str.resize(length + 1);
                  for (size_t i=0; i<length; ++i) {
                      int c;
                      do {
                          c = rand()%128;
                      } while (! isalnum(c));
                      str[i] = (char)c;
                  }
                  str[length-1] = 0;
                  value.setString(&str[0]);
              } else {
                  throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                      << "UnitTestSortArrayPhysical" << "genRandomValue";
              }
              return value;
          }

       void insertMapDataIntoArray(shared_ptr<Query>& query, MemArray& array, CoordValueMap const& m)
       {
           Coordinates coord(1);
           coord[0] = 0;
           vector< shared_ptr<ArrayIterator> > arrayIters(array.getArrayDesc().getAttributes(true).size());
           vector< shared_ptr<ChunkIterator> > chunkIters(arrayIters.size());

           for (size_t i = 0; i < arrayIters.size(); i++)
           {
               arrayIters[i] = array.getIterator(i);
               chunkIters[i] =
                   ((MemChunk&)arrayIters[i]->newChunk(coord)).getIterator(query,
                                                                           ChunkIterator::SEQUENTIAL_WRITE);
           }

           BOOST_FOREACH(CoordValueMapEntry const& p, m) {
               coord[0] = p.first;
               for (size_t i = 0; i < chunkIters.size(); i++)
               {
                   if (!chunkIters[i]->setPosition(coord))
                   {
                       chunkIters[i]->flush();
                       chunkIters[i].reset();
                       chunkIters[i] =
                           ((MemChunk&)arrayIters[i]->newChunk(coord)).getIterator(query,
                                                                                   ChunkIterator::SEQUENTIAL_WRITE);
                       chunkIters[i]->setPosition(coord);
                   }
                   chunkIters[i]->writeItem(p.second);
               }
           }

           for (size_t i = 0; i < chunkIters.size(); i++)
           {
               chunkIters[i]->flush();
           }
       }

       void finalizeSort(shared_ptr<Query>& query)
           {

    	   size_t chunksize = 1000000;
    	   size_t nattrs = 2;
    	     	       	        Coordinate start = 0;
    	     	       	        Coordinate end   = 1000;
    	     	       	        TypeId const type=TID_INT64;
    	     	       	        const int percentEmpty = 20;
    	     	       	        const int percentNullValue = 10;
    	     	       	        const int missingReason = 0;
    	     	       	        uint32_t chunkInterval=100;
                                bool ascent = true;
    	   //ArenaPtr  localarena = newArena(Options("AA").resetting(true).pagesize(1000000000).parent(_parentArena));
    	   // Array schema
    	   vector<AttributeDesc> attributes(nattrs);
    	   for (size_t i = 0; i < nattrs; i++)
    	   {
    		   std::stringstream ss;
    		   ss << "X" << i;
    		   attributes[i] = AttributeDesc((AttributeID)0, ss.str(),  type, AttributeDesc::IS_NULLABLE, 0);
    	   }
    	   vector<DimensionDesc> dimensions(1);
    	   dimensions[0] = DimensionDesc(string("dummy_dimension"), start, end, chunkInterval, 0);
    	   ArrayDesc schema("dummy_array", addEmptyTagAttribute(attributes), dimensions);

    	   // Sort Keys
    	   SortingAttributeInfos sortingAttributeInfos;
    	   SortingAttributeInfo  k;

    	   k.columnNo = 0;
    	   k.ascent = ascent;
    	   sortingAttributeInfos.push_back(k);

    	       	        // Define the array to sort
    	       	        //shared_ptr<MemArray> arrayInst(_spilledParts[0]);
    	       	       // ArrayDesc schema = getArrayDesc();

    	       	        shared_ptr<MemArray> arrayInst(new MemArray(schema,query));

    	       	        shared_ptr<Array> baseArrayInst = static_pointer_cast<MemArray, Array>(arrayInst);

    	       	        //boost::shared_ptr<Query> query = _query;

    	       	        // Generate source data
    	       	        CoordValueMap mapInst;
    	       	        Value value;


    	   				for (Coordinate i=start; i<end+1; ++i) {
    	       	        	if (! rand()%100<percentEmpty) {
    	       	        		mapInst[i] = genRandomValue(type, value, percentNullValue, missingReason);
    	       	        	}
    	       	        }

    	       	        // Insert the map data into the array.
    	       	        insertMapDataIntoArray(query, *arrayInst, mapInst);



    	       	        // Sort
    	       	        const bool preservePositions = false;
    	       	        size_t chunkSize = 1000000;
    	       	        {
    	       	       // SortArray sorter(schema, _arena, preservePositions);
    	       	        }

    	       	     // Sort
    	       	        //const bool preservePositions = false;
    	       	        SortArray sorter(schema, _arena, preservePositions);
    	       	        shared_ptr<TupleComparator> tcomp(new TupleComparator(sortingAttributeInfos, schema));
    	       	        shared_ptr<MemArray> sortedArray = sorter.getSortedArray(baseArrayInst, query, tcomp);


           }


    #define MB 1000000
    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        shared_ptr<Array> inputArray = inputArrays[0];
        shared_ptr<ConstArrayIterator> arrayIter(inputArray->getConstIterator(0));  //everyone has an attribute 0! Even the strangest arrays...
        shared_ptr<ConstChunkIterator> chunkIter;
        size_t numChunks = 0;
        size_t numCells  = 0;

        ArrayDesc const& inputDesc = inputArray->getArrayDesc();
        //SpillingHashCollector collector(256*MB, _arena, inputDesc.getAttributes()[0], query);

        //AttributeComparator comparator(inputDesc.getAttributes()[0].getType());
        //MemoryHashTable memData(comparator, this->_arena);

        while (!arrayIter->end())
        {
            ++numChunks;
            chunkIter = arrayIter->getChunk().getConstIterator();
            while(! chunkIter->end())
            {
                ++numCells;
                //collector.insert(chunkIter->getItem());
                //memData.insert(chunkIter->getItem());
                ++(*chunkIter);
            }
            ++(*arrayIter);
        }

        //collector.finalize();
        //shared_ptr<MemArray> outputMemArray =  collector.finalizeSort();
        finalizeSort(query);




        //PhysicalOperator::dumpArrayToLog(outputMemArray, logger);

        //shared_ptr<Array> outputArray(out);
        //shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
        //Coordinates position(1, query->getInstanceID());
        //shared_ptr<ChunkIterator> outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);


        //shared_ptr<MemArray> redis = collector.makeExchangeArray(out,_schema);
        //shared_ptr<ArrayIterator> outputArrayIter1 = out->getIterator(0);
        //Coordinates position(1, query->getInstanceID());
        //shared_ptr<ChunkIterator> outputChunkIter = outputArrayIter1->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
        //outputChunkIter->setPosition(position);

        /*
        while(! outputChunkIter->end())
        {
        	//Value value;
        	//value.setUint64(chunkIter->getItem());
        	outputChunkIter->writeItem(chunkIter->getItem());
        	outputChunkIter->flush();
        	//chunkIter->getItem().
        	//collector.insert(chunkIter->getItem());
        	++(*chunkIter);
        }
        */


        shared_ptr<Array> outputArray(new MemArray(_schema, query));

        shared_ptr<ArrayIterator> outputArrayIter = outputArray->getIterator(0);
        Coordinates position(1, query->getInstanceID());
        shared_ptr<ChunkIterator> outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
        outputChunkIter->setPosition(position);
        Value value;
        value.setUint64(numChunks);
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();

        outputArrayIter = outputArray->getIterator(1);
        outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        outputChunkIter->setPosition(position);
        value.setUint64(numCells);
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();

        /*
        outputArrayIter = outputArray->getIterator(2);
        outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        outputChunkIter->setPosition(position);
        value.setUint64(collector.returnSize());
        //value.setUint64(memData.size());
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();
        */

        //shared_ptr<MemArray> outfoo = collector.finalize();
        //outfoo.reset();

        return outputArray;

       //return out;
       //return outputArray;

    }
};
REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalGroupedAggregate, "grouped_aggregate", "physical_grouped_aggregate");
} //namespace scidb
