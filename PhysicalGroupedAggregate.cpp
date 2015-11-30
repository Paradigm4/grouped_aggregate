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

namespace grouped_aggregate
{

class StateWriter : public boost::noncopyable
{

private:
    shared_ptr<Array> const _output;
    Coordinates _outputPosition;
    size_t const _numAttributes;
    vector<shared_ptr<ArrayIterator> > _outputArrayIterators;
    vector<shared_ptr<ChunkIterator> > _outputChunkIterators;
    Value _buf;

public:
    static ArrayDesc makeSchema(TypeId const& attributeType, size_t const chunkSize)
    {
        Attributes outputAttributes;
        outputAttributes.push_back( AttributeDesc(0, "bucket", TID_UINT64,    0, 0));
        outputAttributes.push_back( AttributeDesc(1, "hash",   TID_UINT64,    0, 0));
        outputAttributes.push_back( AttributeDesc(2, "value",  attributeType, 0, 0));
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("i", 0, CoordinateBounds::getMax(), chunkSize, 0));
        return ArrayDesc("grouped_aggregate_state", outputAttributes, outputDimensions, defaultPartitioning());
    }

    StateWriter(TypeId const& attributeType, size_t const chunkSize):
        _output(make_shared<MemArray>(schema,query)),
        _outputPosition( splitOnDimension ? 5 : 4, 0),
        _numLiveAttributes(schema.getAttributes(true).size()),
        _outputLineSize(splitOnDimension ? schema.getDimensions()[4].getChunkInterval() : _numLiveAttributes),
        _outputChunkSize(schema.getDimensions()[3].getChunkInterval()),
        _outputArrayIterators(_numLiveAttributes),
        _outputChunkIterators(_numLiveAttributes),
        _splitOnDimension(splitOnDimension),
        _outputColumn(0),
        _attributeDelimiter(attDelimiter)
    {
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            _outputArrayIterators[i] = _output->getIterator(i);
        }
    }

    void newChunk (Coordinates const& inputChunkPosition, shared_ptr<Query>& query)
    {
        _outputPosition[0] = inputChunkPosition[0];
        _outputPosition[1] = inputChunkPosition[1];
        _outputPosition[2] = inputChunkPosition[2];
        _outputPosition[3] = 0;
        if(_splitOnDimension)
        {
            _outputPosition[4] = 0;
        }
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            if(_outputChunkIterators[i].get())
            {
                _outputChunkIterators[i]->flush();
            }
            _outputChunkIterators[i] = _outputArrayIterators[i]->newChunk(_outputPosition).getIterator(query,
                i == 0 ? ChunkIterator::SEQUENTIAL_WRITE : ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        }
    }

    void writeValue (char const* start, char const* end)
    {
        if(_outputColumn < _outputLineSize - 1)
        {
            _buf.setSize(end - start + 1);
            char* d = _buf.getData<char>();
            memcpy(d, start, end-start);
            d[(end-start)]=0;
            if(_splitOnDimension)
            {
                _outputChunkIterators[0] -> setPosition(_outputPosition);
                _outputChunkIterators[0] -> writeItem(_buf);
                ++(_outputPosition[4]);
            }
            else
            {
                _outputChunkIterators[_outputColumn] -> setPosition(_outputPosition);
                _outputChunkIterators[_outputColumn] -> writeItem(_buf);
            }
        }
        else if (_outputColumn == _outputLineSize - 1)
        {
            string value(start, end-start);
            _errorBuf << "long" << _attributeDelimiter << value;
        }
        else
        {
            string value(start, end-start);
            _errorBuf << _attributeDelimiter << value;
        }
        ++_outputColumn;
    }

    void endLine()
    {
        if(_outputColumn < _outputLineSize - 1)
        {
            _buf.setNull();
            if(_splitOnDimension)
            {
                while (_outputColumn < _outputLineSize - 1)
                {
                    _outputChunkIterators[0] -> setPosition(_outputPosition);
                    _outputChunkIterators[0] -> writeItem(_buf);
                    ++(_outputPosition[4]);
                    ++_outputColumn;
                }
            }
            else
            {
                while (_outputColumn < _outputLineSize - 1)
                {
                    _outputChunkIterators[_outputColumn] -> setPosition(_outputPosition);
                    _outputChunkIterators[_outputColumn] -> writeItem(_buf);
                    ++_outputColumn;
                }
            }
            _errorBuf << "short";
        }
        if(_errorBuf.str().size())
        {
            _buf.setString(_errorBuf.str());
        }
        else
        {
            _buf.setNull();
        }
        if(_splitOnDimension)
        {
            _outputChunkIterators[0] -> setPosition(_outputPosition);
            _outputChunkIterators[0] -> writeItem(_buf);
            _outputPosition[4] = 0;
        }
        else
        {
            _outputChunkIterators[_outputLineSize - 1] -> setPosition(_outputPosition);
            _outputChunkIterators[_outputLineSize - 1] -> writeItem(_buf);
        }
        ++(_outputPosition[3]);
        _errorBuf.str("");
        _outputColumn = 0;
    }

    shared_ptr<Array> finalize()
    {
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            if(_outputChunkIterators[i].get())
            {
                _outputChunkIterators[i]->flush();
            }
            _outputChunkIterators[i].reset();
            _outputArrayIterators[i].reset();
        }
        return _output;
    }
};

} //namespace grouped_aggregate

using namespace std;
using namespace grouped_aggregate
using std::shared_ptr;

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

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const&, std::vector<ArrayDesc> const&) const
    {
        return RedistributeContext(psUndefined);
    }

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        shared_ptr<Array> inputArray = inputArrays[0];


        AttributeDesc const& attrDesc = inputArray->getArrayDesc().getAttributes()[0];
        ArenaPtr operatorArena = this->getArena();

        ArenaPtr hashArena(newArena(Options("").resetting(true).pagesize(10 * 1024 * 1204).parent(operatorArena)));
        AttributeComparator cmp (attrDesc.getType());
        MemoryHashTable mht(cmp, hashArena);

        size_t numChunks = 0;
        size_t numCells  = 0;
        size_t numUnique = 0;

        mht.dumpStatsToLog();

        shared_ptr<ConstArrayIterator> arrayIter(inputArray->getConstIterator(0));
        shared_ptr<ConstChunkIterator> chunkIter;
        while (!arrayIter->end())
        {
            ++numChunks;
            chunkIter = arrayIter->getChunk().getConstIterator();
            while(! chunkIter->end())
            {
                Value const& v = chunkIter->getItem();
                mht.insert(v);
                ++numCells;
                ++(*chunkIter);
            }
            ++(*arrayIter);
        }

        mht.dumpStatsToLog();

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
        value.setUint64(mht.numValues());
        outputChunkIter->writeItem(value);
        outputChunkIter->flush();

        return outputArray;
    }
};
REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalGroupedAggregate, "grouped_aggregate", "physical_grouped_aggregate");
} //namespace scidb
