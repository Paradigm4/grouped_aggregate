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
#include <array/SortArray.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory>
#include <cstddef>

#include "AggregateHashTable.h"
#include "GroupedAggregateSettings.h"

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("agg"));


using namespace boost;
using namespace std;

namespace scidb
{

namespace grouped_aggregate
{

class FlatWriter : public boost::noncopyable
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
    static ArrayDesc makeSchema(TypeId const& groupType, TypeId const& valueType, size_t const chunkSize)
    {
        Attributes outputAttributes;
        outputAttributes.push_back( AttributeDesc(0, "hash",   TID_UINT64,    0, 0));
        outputAttributes.push_back( AttributeDesc(1, "group",  groupType, 0, 0));
        outputAttributes.push_back( AttributeDesc(2, "value",  valueType,     AttributeDesc::IS_NULLABLE, 0));
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("i", 0, CoordinateBounds::getMax(), chunkSize, 0));
        return ArrayDesc("grouped_aggregate_state", outputAttributes, outputDimensions, defaultPartitioning());
    }

    FlatWriter(TypeId const& groupType, TypeId const& valueType, size_t const chunkSize, shared_ptr<Query> const& query):
        _output(make_shared<MemArray>(makeSchema(groupType, valueType, chunkSize), query)),
        _numAttributes(3),
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

    void writeValue (uint64_t const hash, Value const& group, Value const& value)
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
        _outputChunkIterators[1]->writeItem(group);
        _outputChunkIterators[2]->setPosition(_outputPosition);
        _outputChunkIterators[2]->writeItem(value);
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

template <bool FINAL_RESULT = false>
class MergeWriter : public boost::noncopyable
{
private:
    shared_ptr<Array> const _output;
    size_t const _numAttributes;
    size_t const _chunkSize;
    size_t const _numInstances;
    InstanceID const _myInstanceId;
    AggregatePtr     _aggregate;
    vector<uint64_t> _hashBreaks;
    size_t _currentBreak;
    shared_ptr<Query> _query;
    Coordinates _outputPosition;
    vector<shared_ptr<ArrayIterator> > _outputArrayIterators;
    vector<shared_ptr<ChunkIterator> > _outputChunkIterators;
    Value    _curHash;
    Value    _curGroup;
    Value    _curState;

public:
    static ArrayDesc makeSchema(TypeId const& groupType, TypeId const& stateType, size_t const chunkSize, size_t const numInstances, bool final_result)
    {
        Attributes outputAttributes;
        size_t i =0;
        if(!final_result)
        {
            outputAttributes.push_back( AttributeDesc(i++, "hash",   TID_UINT64,    0, 0));
        }
        outputAttributes.push_back( AttributeDesc(i++, "group",  groupType,     0, 0));
        outputAttributes.push_back( AttributeDesc(i++, "result",  stateType,     AttributeDesc::IS_NULLABLE, 0));
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("dst_instance_id", 0, numInstances-1, 1, 0));
        outputDimensions.push_back(DimensionDesc("src_instance_id", 0, numInstances-1, 1, 0));
        outputDimensions.push_back(DimensionDesc("value_no",        0, CoordinateBounds::getMax(), chunkSize, 0));
        return ArrayDesc("grouped_aggregate_state", outputAttributes, outputDimensions, defaultPartitioning());
    }

    MergeWriter(TypeId const& attributeType, TypeId const& stateType, size_t const chunkSize, shared_ptr<Query> const& query, AggregatePtr& aggregate):
        _output(make_shared<MemArray>(makeSchema(attributeType, stateType, chunkSize, query->getInstancesCount(), FINAL_RESULT), query)),
        _numAttributes(FINAL_RESULT ? 2 : 3),
        _chunkSize(chunkSize),
        _numInstances(query->getInstancesCount()),
        _myInstanceId(query->getInstanceID()),
        _aggregate(aggregate),
        _hashBreaks(_numInstances-1,0),
        _query(query),
        _outputPosition(3, 0),
        _outputArrayIterators(_numAttributes),
        _outputChunkIterators(_numAttributes)
    {
        _curHash.setNull(0);
        _curGroup.setNull(0);
        _curState.setNull(0);
        uint64_t break_interval = std::numeric_limits<uint64_t>::max() / _numInstances; //XXX:CAN'T DO EASY ROUNDOFF
        LOG4CXX_DEBUG(logger, "BREAK_INTERVAL: "<<break_interval)
        for(size_t i=0; i<_numInstances-1; ++i)
        {
            _hashBreaks[i] = break_interval * (i+1);
            LOG4CXX_DEBUG(logger, "HASH_BREAKS: "<<i<<" "<<_hashBreaks[i])
        }
        _currentBreak = 0;
        _outputPosition[0] = 0;
        _outputPosition[1] = _myInstanceId;
        _outputPosition[2] = 0;
        for(AttributeID i =0; i<_numAttributes; ++i)
        {
            _outputArrayIterators[i] = _output->getIterator(i);
        }
    }

    void writeValue (uint64_t const hash, Value const& group, Value const& item)
    {
        Value buf;
        buf.setUint64(hash);
        writeValue(buf, group, item);
    }

    void writeValue (Value const& hash, Value const& group, Value const& item)
    {
        if(_curHash.getMissingReason() == 0 || _curHash.getUint64() != hash.getUint64() || _curGroup != group)
        {
            if(_curHash.getMissingReason() != 0)
            {
                writeCurrent();
            }
            _curHash = hash;
            _curGroup = group;
            _aggregate->initializeState(_curState);
        }
        _aggregate->accumulateIfNeeded(_curState, item);
    }

    void writeState (uint64_t const hash, Value const& group, Value const& state)
    {
        Value buf;
        buf.setUint64(hash);
        writeState(buf, group, state);
    }

    void writeState (Value const& hash, Value const& group, Value const& state)
    {
        if(_curHash.getMissingReason() == 0 || _curHash.getUint64() != hash.getUint64() || _curGroup != group)
        {
            if(_curHash.getMissingReason() != 0)
            {
                writeCurrent();
            }
            _curHash = hash;
            _curGroup = group;
            _aggregate->initializeState(_curState);
        }
        _aggregate->mergeIfNeeded(_curState, state);
    }

private:
    void writeCurrent()
    {
        //gonna do a write, then!
        while(_currentBreak < _numInstances - 1 && _curHash.getUint64() > _hashBreaks[_currentBreak] )
        {
            ++_currentBreak;
        }
        bool newChunk = false;
        if ( static_cast<Coordinate>(_currentBreak) != _outputPosition[0])
        {
            _outputPosition[0] = _currentBreak;
            _outputPosition[2] = 0;
            newChunk = true;
        }
        else if(_outputPosition[2] % _chunkSize == 0)
        {
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
                                                i == 0 ? ChunkIterator::SEQUENTIAL_WRITE : ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
            }
        }
        size_t i = 0;
        if(!FINAL_RESULT)
        {
            _outputChunkIterators[i]->setPosition(_outputPosition);
            _outputChunkIterators[i]->writeItem(_curHash);
            i++;
        }
        _outputChunkIterators[i]->setPosition(_outputPosition);
        _outputChunkIterators[i]->writeItem(_curGroup);
        i++;
        _outputChunkIterators[i]->setPosition(_outputPosition);
        if(FINAL_RESULT)
        {
            Value result;
            _aggregate->finalResult(result, _curState);
            _outputChunkIterators[i]->writeItem(result);
        }
        else
        {
            _outputChunkIterators[i]->writeItem(_curState);
        }
        ++(_outputPosition[2]);
    }

public:
    shared_ptr<Array> finalize()
    {
        if(_curHash.getMissingReason() != 0)
        {
            writeCurrent();
        }
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

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        grouped_aggregate::Settings settings(inputArrays[0]->getArrayDesc(), _parameters, true, query);
        AttributeID const groupAttribute = settings.getGroupAttributeId();
        AttributeID const aggregatedAttribute = settings.getInputAttributeId();
        AttributeComparator comparator( settings.getGroupAttributeType());
        ArenaPtr operatorArena = this->getArena();
        ArenaPtr hashArena(newArena(Options("").resetting(true).pagesize(10 * 1024 * 1204).parent(operatorArena)));
        AggregateHashTable aht(comparator, hashArena);
        AggregatePtr agg = settings.cloneAggregate();
        shared_ptr<ConstArrayIterator> gaiter(inputArrays[0]->getConstIterator(settings.getGroupAttributeId()));
        shared_ptr<ConstArrayIterator> iaiter(inputArrays[0]->getConstIterator(settings.getInputAttributeId()));
        shared_ptr<ConstChunkIterator> gciter, iciter;
        FlatWriter flatWriter(settings.getGroupAttributeType(), settings.getInputAttributeType(), 1000000, query);
        size_t const maxTableSize = 150*1024*1024;
        while(!gaiter->end())
        {
            gciter=gaiter->getChunk().getConstIterator();
            iciter=iaiter->getChunk().getConstIterator();
            while(!gciter->end())
            {
                uint64_t hash;
                Value const& group = gciter->getItem();
                Value const& input = iciter->getItem();
                if(aht.usedBytes() < maxTableSize)
                {
                    aht.insert(group, input, agg);
                }
                else
                {
                    if(!aht.contains(group, hash))
                    {
                        flatWriter.writeValue(hash, group, input);
                    }
                    else
                    {
                        aht.insert(group, input, agg);
                    }
                }
                ++(*gciter);
                ++(*iciter);
            }
            ++(*gaiter);
            ++(*iaiter);
        }
        shared_ptr<Array> arr = flatWriter.finalize();
        aht.sortKeys();
        SortingAttributeInfos sortingAttributeInfos(2);
        sortingAttributeInfos[0].columnNo = 0;
        sortingAttributeInfos[0].ascent = true;
        sortingAttributeInfos[1].columnNo = 1;
        sortingAttributeInfos[1].ascent = true;
        const bool preservePositions = false;
        SortArray sorter(arr->getArrayDesc(), _arena, preservePositions, 1000000);
        shared_ptr<TupleComparator> tcomp(make_shared<TupleComparator>(sortingAttributeInfos, arr->getArrayDesc()));
        arr = sorter.getSortedArray(arr, query, tcomp);
        shared_ptr<ConstArrayIterator> haiter(arr->getConstIterator(0));
        gaiter = arr->getConstIterator(1);
        iaiter = arr->getConstIterator(2);
        shared_ptr<ConstChunkIterator> hciter;
        AggregateHashTable::const_iterator ahtIter = aht.getIterator();
        MergeWriter<false> mergeWriter(settings.getGroupAttributeType(), settings.getStateType(), 1000000, query, agg);
        while(!haiter->end())
        {
            hciter = haiter->getChunk().getConstIterator();
            gciter = gaiter->getChunk().getConstIterator();
            iciter = iaiter->getChunk().getConstIterator();
            while(!hciter->end())
            {
                Value const& hash  = hciter->getItem();
                Value const& group = gciter->getItem();
                Value const& input = iciter->getItem();
                while(!ahtIter.end() && (ahtIter.getCurrentHash() < hash.getUint64() || (ahtIter.getCurrentHash() == hash.getUint64() && comparator(ahtIter.getCurrentGroup(), group))))
                {
                    mergeWriter.writeState(ahtIter.getCurrentHash(), ahtIter.getCurrentGroup(), ahtIter.getCurrentState());
                    ahtIter.next();
                }
                mergeWriter.writeValue(hash,group,input);
                ++(*hciter);
                ++(*gciter);
                ++(*iciter);
            }
            ++(*haiter);
            ++(*gaiter);
            ++(*iaiter);
        }
        size_t count =0;
        while(!ahtIter.end())
        {
            mergeWriter.writeState(ahtIter.getCurrentHash(), ahtIter.getCurrentGroup(), ahtIter.getCurrentState());
            ahtIter.next();
            ++count;
        }
        LOG4CXX_DEBUG(logger, "COUNT: "<<count);
        arr = mergeWriter.finalize();
        arr = redistributeToRandomAccess(arr, query, psByRow, ALL_INSTANCE_MASK, std::shared_ptr<CoordinateTranslator>(), 0, std::shared_ptr<PartitioningSchemaData>());
        MergeWriter<true> output(settings.getGroupAttributeType(), settings.getResultType(), 1000000, query, agg);
        size_t const numInstances = query->getInstancesCount();
        vector<shared_ptr<ConstArrayIterator> > haiters(numInstances);
        vector<shared_ptr<ConstArrayIterator> > gaiters(numInstances);
        vector<shared_ptr<ConstArrayIterator> > vaiters(numInstances);
        vector<shared_ptr<ConstChunkIterator> > hciters(numInstances);
        vector<shared_ptr<ConstChunkIterator> > gciters(numInstances);
        vector<shared_ptr<ConstChunkIterator> > vciters(numInstances);
        vector<Coordinates > positions(numInstances);
        size_t numClosed = 0;
        for(size_t inst =0; inst<numInstances; ++inst)
        {
            positions[inst].resize(3);
            positions[inst][0] = query->getInstanceID();
            positions[inst][1] = inst;
            positions[inst][2] = 0;
            haiters[inst] = arr->getConstIterator(0);
            if(!haiters[inst]->setPosition(positions[inst]))
            {
                haiters[inst].reset();
                gaiters[inst].reset();
                vaiters[inst].reset();
                hciters[inst].reset();
                gciters[inst].reset();
                vciters[inst].reset();
                numClosed++;
            }
            else
            {
                gaiters[inst] = arr->getConstIterator(1);
                gaiters[inst]->setPosition(positions[inst]);
                vaiters[inst] = arr->getConstIterator(2);
                vaiters[inst]->setPosition(positions[inst]);
                hciters[inst] = haiters[inst]->getChunk().getConstIterator();
                gciters[inst] = gaiters[inst]->getChunk().getConstIterator();
                vciters[inst] = vaiters[inst]->getChunk().getConstIterator();
            }
        }
        while(numClosed < numInstances)
        {
            bool minHashSet = false;
            uint64_t minHash=0;
            Value minGroup;
            for(size_t inst=0; inst<numInstances; ++inst)
            {
                if(hciters[inst] == 0)
                {
                    continue;
                }
                uint64_t hash    = hciters[inst]->getItem().getUint64();
                Value const& grp = gciters[inst]->getItem();
                if(!minHashSet || (hash < minHash || (hash == minHash && comparator(grp, minGroup))))
                {
                    minHash = hash;
                    minGroup = grp;
                    minHashSet = true;
                }
            }
            for(size_t inst=0; inst<numInstances; ++inst)
            {
                if(hciters[inst] == 0)
                {
                    continue;
                }
                uint64_t hash    = hciters[inst]->getItem().getUint64();
                Value const& grp = gciters[inst]->getItem();
                Value const& val = vciters[inst]->getItem();
                if(hash == minHash && grp == minGroup)
                {
                    output.writeState(hash, grp, val);
                    ++(*hciters[inst]);
                    ++(*gciters[inst]);
                    ++(*vciters[inst]);
                    if(hciters[inst]->end())
                    {
                        positions[inst][2] = positions[inst][2] + 1000000; //TODO: setting-ize
                        bool sp = haiters[inst]->setPosition(positions[inst]);
                        if(!sp)
                        {
                            haiters[inst].reset();
                            gaiters[inst].reset();
                            vaiters[inst].reset();
                            hciters[inst].reset();
                            gciters[inst].reset();
                            vciters[inst].reset();
                            numClosed++;
                        }
                        else
                        {
                            gaiters[inst]->setPosition(positions[inst]);
                            vaiters[inst]->setPosition(positions[inst]);
                            hciters[inst] = haiters[inst]->getChunk().getConstIterator();
                            gciters[inst] = gaiters[inst]->getChunk().getConstIterator();
                            vciters[inst] = vaiters[inst]->getChunk().getConstIterator();
                        }
                    }
                }
            }
        }
        return output.finalize();
    }
};
REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalGroupedAggregate, "grouped_aggregate", "physical_grouped_aggregate");
} //namespace scidb
