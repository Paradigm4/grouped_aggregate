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
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <log4cxx/logger.h>
#include <util/NetworkMessage.h>
#include <array/RLE.h>
#include <array/SortArray.h>

#include "query/Operator.h"
#include "HashTableUtilities.h"
#include <array/SortArray.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory>
#include <cstddef>

using namespace boost;
using namespace std;

namespace scidb
{

namespace grouped_aggregate
{

class StateWriter : public boost::noncopyable
{
private:
    shared_ptr<Array> const _output;
    size_t const _numAttributes;
    size_t const _chunkSize;
    shared_ptr<Query> _query;
    Coordinates _outputPosition;
    vector<shared_ptr<ArrayIterator> > _outputArrayIterators;
    vector<shared_ptr<ChunkIterator> > _outputChunkIterators;
    Value _buf;

public:
    static ArrayDesc makeSchema(TypeId const& attributeType, size_t const chunkSize)
    {
        Attributes outputAttributes;
        outputAttributes.push_back( AttributeDesc(0, "hash",   TID_UINT64,    0, 0));
        outputAttributes.push_back( AttributeDesc(1, "value",  attributeType, 0, 0));
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("i", 0, CoordinateBounds::getMax(), chunkSize, 0));
        return ArrayDesc("grouped_aggregate_state", outputAttributes, outputDimensions, defaultPartitioning());
    }

    StateWriter(TypeId const& attributeType, size_t const chunkSize, shared_ptr<Query> const& query):
        _output(make_shared<MemArray>(makeSchema(attributeType, chunkSize), query)),
        _numAttributes(2),
        _chunkSize(chunkSize),
        _query(query),
        _outputPosition(1, 0),
        _outputArrayIterators(_numAttributes),
        _outputChunkIterators(_numAttributes)
    {
        for(AttributeID i =0; i<_numAttributes; ++i)
        {
            _outputArrayIterators[i] = _output->getIterator(i);
        }
    }

    void writeValue (uint64_t const hash, Value const& v)
    {
        if(_outputPosition[0] % _chunkSize == 0)
        {
            for(AttributeID i=0; i<_numAttributes; ++i)
            {
                if(_outputChunkIterators[i].get())
                {
                    _outputChunkIterators[i]->flush();
                }
                _outputChunkIterators[i] = _outputArrayIterators[i]->newChunk(_outputPosition).getIterator(_query,
                                i == 0 ? ChunkIterator::SEQUENTIAL_WRITE :
                                         ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
            }
        }
        _buf.setUint64(hash);
        _outputChunkIterators[0]->setPosition(_outputPosition);
        _outputChunkIterators[0]->writeItem(_buf);
        _outputChunkIterators[1]->setPosition(_outputPosition);
        _outputChunkIterators[1]->writeItem(v);
        ++(_outputPosition[0]);
    }

    shared_ptr<Array> finalize()
    {
        for(AttributeID i =0; i<_numAttributes; ++i)
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


class MergeWriter : public boost::noncopyable
{
private:
    shared_ptr<Array> const _output;
    size_t const _numAttributes;
    size_t const _chunkSize;
    size_t const _numInstances;
    InstanceID const _myInstanceId;
    vector<uint64_t> _hashBreaks;
    size_t _currentBreak;
    shared_ptr<Query> _query;
    Coordinates _outputPosition;
    vector<shared_ptr<ArrayIterator> > _outputArrayIterators;
    vector<shared_ptr<ChunkIterator> > _outputChunkIterators;
    uint64_t _prevHash;
    Value    _prevVal;

public:
    static ArrayDesc makeSchema(TypeId const& attributeType, size_t const chunkSize, size_t const numInstances)
    {
        Attributes outputAttributes;
        outputAttributes.push_back( AttributeDesc(0, "hash",   TID_UINT64,    0, 0));
        outputAttributes.push_back( AttributeDesc(1, "value",  attributeType, 0, 0));
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("dst_instance_id", 0, numInstances-1, 1, 0));
        outputDimensions.push_back(DimensionDesc("src_instance_id", 0, numInstances-1, 1, 0));
        outputDimensions.push_back(DimensionDesc("block_no",        0, CoordinateBounds::getMax(), 1, 0));
        outputDimensions.push_back(DimensionDesc("value_no",        0, chunkSize-1, chunkSize, 0));
        return ArrayDesc("grouped_aggregate_state", outputAttributes, outputDimensions, defaultPartitioning());
    }

    MergeWriter(TypeId const& attributeType, size_t const chunkSize, shared_ptr<Query> const& query):
        _output(make_shared<MemArray>(makeSchema(attributeType, chunkSize, query->getInstancesCount()), query)),
        _numAttributes(2),
        _chunkSize(chunkSize),
        _numInstances(query->getInstancesCount()),
        _myInstanceId(query->getInstanceID()),
        _hashBreaks(_numInstances-1,0),
        _query(query),
        _outputPosition(4, 0),
        _outputArrayIterators(_numAttributes),
        _outputChunkIterators(_numAttributes),
        _prevHash(0)
    {
        _prevVal.setNull(0);
        uint64_t break_interval = std::numeric_limits<uint64_t>::max() / _numInstances; //XXX:CAN'T DO EASY ROUNDOFF
        for(size_t i=0; i<_numInstances-1; ++i)
        {
            _hashBreaks[i] = break_interval * (i+1);
        }
        _currentBreak = 0;
        _outputPosition[0] = 0;
        _outputPosition[1] = _myInstanceId;
        _outputPosition[2] = -1;
        _outputPosition[3] = 0;
        for(AttributeID i =0; i<_numAttributes; ++i)
        {
            _outputArrayIterators[i] = _output->getIterator(i);
        }
    }

    void writeValue (uint64_t const hash, Value const& v)
    {
        Value buf;
        buf.setUint64(hash);
        writeValue(buf, v);
    }

    void writeValue (Value const& hash, Value const& v)
    {
        uint64_t const h = hash.getUint64();
        if(_prevVal.getMissingReason() != 0 && _prevHash == h && _prevVal == v)
        {
            return;
        }
        //gonna do a write, then!
        while(h > _hashBreaks[_currentBreak] && _currentBreak < _numInstances - 1)
        {
            ++_currentBreak;
        }
        bool newChunk = false;
        if ( static_cast<Coordinate>(_currentBreak) != _outputPosition[0])
        {
            _outputPosition[0] = _currentBreak;
            _outputPosition[2] = 0;
            _outputPosition[3] = 0;
            newChunk = true;
        }
        else if(_outputPosition[3] % _chunkSize == 0)
        {
            ++(_outputPosition[2]);
            _outputPosition[3] = 0;
            newChunk = true;
        }
        if( newChunk )
        {
            for(AttributeID i=0; i<_numAttributes; ++i)
            {
                if(_outputChunkIterators[i].get())
                {
                    _outputChunkIterators[i]->flush();
                }
                _outputChunkIterators[i] = _outputArrayIterators[i]->newChunk(_outputPosition).getIterator(_query,
                                i == 0 ? ChunkIterator::SEQUENTIAL_WRITE :
                                         ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
            }
        }
        _outputChunkIterators[0]->setPosition(_outputPosition);
        _outputChunkIterators[0]->writeItem(hash);
        _outputChunkIterators[1]->setPosition(_outputPosition);
        _outputChunkIterators[1]->writeItem(v);
        ++(_outputPosition[3]);
        _prevHash = h;
        _prevVal  = v;
    }

    shared_ptr<Array> finalize()
    {
        for(AttributeID i =0; i<_numAttributes; ++i)
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

using namespace grouped_aggregate;

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

    shared_ptr<Array> preFlatten(shared_ptr<Array> const& inputArray, shared_ptr<Query> const& query)
    {
        AttributeDesc const& attrDesc = inputArray->getArrayDesc().getAttributes()[0];
        ArenaPtr operatorArena = this->getArena();
        ArenaPtr hashArena(newArena(Options("").resetting(true).pagesize(10 * 1024 * 1204).parent(operatorArena)));
        AttributeComparator cmp (attrDesc.getType());
        MemoryHashTable mht(cmp, hashArena);
        bool exhausted = false;
        size_t const maxTableBytes = 128 * 1024 * 1024;
        StateWriter spillover (attrDesc.getType(), 1024*1024, query);
        shared_ptr<ConstArrayIterator> arrayIter(inputArray->getConstIterator(0));
        shared_ptr<ConstChunkIterator> chunkIter;
        while (!arrayIter->end())
        {
            chunkIter = arrayIter->getChunk().getConstIterator();
            while(! chunkIter->end())
            {
                Value const& v = chunkIter->getItem();
                if (!exhausted)
                {
                    bool inserted = mht.insert(v);
                    if(inserted && mht.usedBytes() > maxTableBytes)
                    {
                        LOG4CXX_DEBUG(logger, "Table exhausted!");
                        mht.dumpStatsToLog();
                        exhausted = true;
                    }
                }
                else
                {
                    uint64_t hash;
                    if(!mht.contains(v, hash))
                    {
                        spillover.writeValue(hash, v);
                    }
                }
                ++(*chunkIter);
            }
            ++(*arrayIter);
        }
        MemoryHashTable::const_iterator iter = mht.getIterator();
        while( !iter.end() )
        {
            spillover.writeValue(iter.getCurrentHash(), iter.getCurrentItem());
            iter.next();
        }
        return spillover.finalize();
    }

    shared_ptr<Array> condense (shared_ptr<Array> const& inputArray, shared_ptr<Query> const& query)
    {
        shared_ptr<Array> arr = preFlatten(inputArray, query);
        SortingAttributeInfos sortingAttributeInfos(2);
        sortingAttributeInfos[0].columnNo = 0;
        sortingAttributeInfos[0].ascent = true;
        sortingAttributeInfos[1].columnNo = 1;
        sortingAttributeInfos[1].ascent = true;
        const bool preservePositions = false;
        SortArray sorter(arr->getArrayDesc(),
                         _arena,
                         preservePositions,
                         1000000);
        std::shared_ptr<TupleComparator> tcomp(std::make_shared<TupleComparator>(sortingAttributeInfos, arr->getArrayDesc()));
        arr = sorter.getSortedArray(arr, query, tcomp);
        MergeWriter merger(inputArray->getArrayDesc().getAttributes()[0].getType(), 1000000, query);
        shared_ptr<ConstArrayIterator> haiter = arr->getConstIterator(0);
        shared_ptr<ConstArrayIterator> vaiter = arr->getConstIterator(1);
        while(!haiter->end())
        {
            shared_ptr<ConstChunkIterator> hciter = haiter->getChunk().getConstIterator();
            shared_ptr<ConstChunkIterator> vciter = vaiter->getChunk().getConstIterator();
            while(!hciter->end())
            {
                Value const& hash = hciter->getItem();
                Value const& val  = vciter->getItem();
                merger.writeValue(hash, val);
                ++(*hciter);
                ++(*vciter);
            }
            ++(*haiter);
            ++(*vaiter);
        }
        return merger.finalize();
    }


    shared_ptr<Array> shuffleMerge(shared_ptr<Array>& inputArray, shared_ptr<Query> const& query)
    {
        TypeId const inputType = inputArray->getArrayDesc().getAttributes()[0].getType();
        shared_ptr<Array> redist = redistributeToRandomAccess(inputArray, query, psByRow, ALL_INSTANCE_MASK,
                                                                 std::shared_ptr<CoordinateTranslator>(),
                                                                 0,
                                                                 std::shared_ptr<PartitioningSchemaData>());
        AttributeComparator attComp(inputType);
        MergeWriter output(inputType, 1000000, query);
        size_t const numInstances = query->getInstancesCount();
        vector<shared_ptr<ConstArrayIterator> > haiters(numInstances);
        vector<shared_ptr<ConstArrayIterator> > vaiters(numInstances);
        vector<shared_ptr<ConstChunkIterator> > hciters(numInstances);
        vector<shared_ptr<ConstChunkIterator> > vciters(numInstances);
        vector<Coordinates > positions(numInstances);
        size_t numClosed = 0;
        for(size_t inst =0; inst<numInstances; ++inst)
        {
            positions[inst].resize(4);
            positions[inst][0] = query->getInstanceID();
            positions[inst][1] = inst;
            positions[inst][2] = 0;
            positions[inst][3] = 0;
            haiters[inst] = redist->getConstIterator(0);
            if(!haiters[inst]->setPosition(positions[inst]))
            {
                haiters[inst].reset();
                vaiters[inst].reset();
                hciters[inst].reset();
                vciters[inst].reset();
                numClosed++;
            }
            else
            {
                vaiters[inst] = redist->getConstIterator(1);
                vaiters[inst]->setPosition(positions[inst]);
                hciters[inst] = haiters[inst]->getChunk().getConstIterator();
                vciters[inst] = vaiters[inst]->getChunk().getConstIterator();
            }
        }
        while(numClosed < numInstances)
        {
            bool minHashSet = false;
            uint64_t minHash=0;
            Value minItem;
            for(size_t inst=0; inst<numInstances; ++inst)
            {
                if(hciters[inst] == 0)
                {
                    continue;
                }
                uint64_t hash = hciters[inst]->getItem().getUint64();
                Value const& val = vciters[inst]->getItem();
                if(!minHashSet || (hash < minHash || (hash == minHash && attComp(val, minItem))))
                {
                    minHash = hash;
                    minItem = val;
                    minHashSet = true;
                }
            }
            output.writeValue(minHash, minItem);
            for(size_t inst=0; inst<numInstances; ++inst)
            {
                if(hciters[inst] == 0)
                {
                    continue;
                }
                uint64_t hash = hciters[inst]->getItem().getUint64();
                Value const& val = vciters[inst]->getItem();
                if(hash == minHash && val == minItem)
                {
                    ++(*hciters[inst]);
                    ++(*vciters[inst]);
                    if(hciters[inst]->end())
                    {
                        ++(positions[inst][2]);
                        positions[inst][3] = 0;
                        bool sp = haiters[inst]->setPosition(positions[inst]);
                        if(!sp)
                        {
                            haiters[inst].reset();
                            vaiters[inst].reset();
                            hciters[inst].reset();
                            vciters[inst].reset();
                            numClosed++;
                        }
                        else
                        {
                            vaiters[inst]->setPosition(positions[inst]);
                            hciters[inst] = haiters[inst]->getChunk().getConstIterator();
                            vciters[inst] = vaiters[inst]->getChunk().getConstIterator();
                        }
                    }
                }
            }
        }
        return output.finalize();
    }

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        shared_ptr<Array> result = condense(inputArrays[0], query);
        result = shuffleMerge(result, query);
        return result;
    }
};
REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalGroupedAggregate, "grouped_aggregate", "physical_grouped_aggregate");
} //namespace scidb
