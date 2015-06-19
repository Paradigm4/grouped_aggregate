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
#include "HashTableUtilities.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

namespace scidb
{

class PhysicalGroupedAggregate : public PhysicalOperator
{
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

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        shared_ptr<Array> inputArray = inputArrays[0];
        shared_ptr<ConstArrayIterator> arrayIter(inputArray->getConstIterator(0));  //everyone has an attribute 0! Even the strangest arrays...
        shared_ptr<ConstChunkIterator> chunkIter;
        size_t numChunks = 0;
        size_t numCells  = 0;

        ArrayDesc const& inputDesc = inputArray->getArrayDesc();
        //SpillingHashCollector collector(256*MB, this->_arena, inputDesc.getAttributes()[0], query);

        AttributeComparator comparator(inputDesc.getAttributes()[0].getType());
        MemoryHashTable memData(comparator, this->_arena);

        while (!arrayIter->end())
        {
            ++numChunks;
            chunkIter = arrayIter->getChunk().getConstIterator();
            while(! chunkIter->end())
            {
                ++numCells;
                //collector.insert(chunkIter->getItem());
                memData.insert(chunkIter->getItem());
                ++(*chunkIter);
            }
            ++(*arrayIter);
        }

        //shared_ptr<Array> outputArray =  collector.finalizeArray();


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

        outputArrayIter = outputArray->getIterator(2);
        outputChunkIter = outputArrayIter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        outputChunkIter->setPosition(position);
        //value.setUint64(collector.returnSize());
        value.setUint64(memData.size());
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();


        //shared_ptr<MemArray> outfoo = collector.finalize();
        //outfoo.reset();

        return outputArray;

       //return out;
       //return outputArray;

    }
};
REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalGroupedAggregate, "grouped_aggregate", "physical_grouped_aggregate");
} //namespace scidb
