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

using grouped_aggregate::Settings;

namespace grouped_aggregate
{

template <Settings::SchemaType SCHEMA_TYPE>
class MergeWriter : public boost::noncopyable
{
private:
    shared_ptr<Array> _output;
    size_t const _numAttributes;
    size_t const _chunkSize;
    size_t const _numInstances;
    InstanceID const _myInstanceId;
    AggregatePtr     _aggregate;
    vector<uint64_t> _hashBreaks;
    size_t _currentBreak;
    shared_ptr<Query> _query;
    Coordinates _outputPosition;
    Coordinate& _outputValueNo;
    vector<shared_ptr<ArrayIterator> > _outputArrayIterators;
    vector<shared_ptr<ChunkIterator> > _outputChunkIterators;
    Value    _curHash;
    Value    _curGroup;
    Value    _curState;

public:
    MergeWriter(Settings const& settings, shared_ptr<Query> const& query, string const name = ""):
        _output(make_shared<MemArray>(settings.makeSchema(SCHEMA_TYPE, name), query)),
        _numAttributes(SCHEMA_TYPE == Settings:: FINAL ? 2 : 3),
        _chunkSize(_output->getArrayDesc().getDimensions()[_output->getArrayDesc().getDimensions().size()-1].getChunkInterval()),
        _numInstances(query->getInstancesCount()),
        _myInstanceId(query->getInstanceID()),
        _aggregate(settings.cloneAggregate()),
        _hashBreaks(_numInstances-1,0),
        _query(query),
        _outputPosition( SCHEMA_TYPE == Settings::SPILL ? 1 :
                         SCHEMA_TYPE== Settings::MERGE ?  3 :
                                                          2 , 0),
        _outputValueNo(  SCHEMA_TYPE == Settings::SPILL ? _outputPosition[0] :
                         SCHEMA_TYPE == Settings::MERGE ? _outputPosition[2] :
                                                          _outputPosition[1]),
        _outputArrayIterators(_numAttributes),
        _outputChunkIterators(_numAttributes)
    {
        _curHash.setNull(0);
        _curGroup.setNull(0);
        _curState.setNull(0);
        uint64_t break_interval = std::numeric_limits<uint64_t>::max() / _numInstances; //XXX:CAN'T DO EASY ROUNDOFF
        for(size_t i=0; i<_numInstances-1; ++i)
        {
            _hashBreaks[i] = break_interval * (i+1);
        }
        _currentBreak = 0;
        if(SCHEMA_TYPE == Settings::MERGE)
        {
            _outputPosition[0] = 0;
            _outputPosition[1] = _myInstanceId;
            _outputPosition[2] = 0;
        }
        else if(SCHEMA_TYPE == Settings::FINAL)
        {
            _outputPosition[0] = _myInstanceId;
            _outputPosition[1] = 0;
        }
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
        if(SCHEMA_TYPE == Settings::SPILL && _curHash.getMissingReason() != 0)
        {
            writeCurrent();
            _curHash = hash;
            _curGroup = group;
            _curState = item;
        }
        else
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
    }

    void writeState (uint64_t const hash, Value const& group, Value const& state)
    {
        Value buf;
        buf.setUint64(hash);
        writeState(buf, group, state);
    }

    void writeState (Value const& hash, Value const& group, Value const& state)
    {
        if(SCHEMA_TYPE == Settings::SPILL)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "don't call writeState on a SPILL writer";
        }
        else
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
    }

private:
    void writeCurrent()
    {
        //gonna do a write, then!
        while( SCHEMA_TYPE == Settings::MERGE && _currentBreak < _numInstances - 1 && _curHash.getUint64() > _hashBreaks[_currentBreak] )
        {
            ++_currentBreak;
        }
        bool newChunk = false;
        if ( SCHEMA_TYPE == Settings::MERGE && static_cast<Coordinate>(_currentBreak) != _outputPosition[0])
        {
            _outputPosition[0] = _currentBreak;
            _outputPosition[2] = 0;
            newChunk = true;
        }
        else if( _outputValueNo % _chunkSize == 0)
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
        if(SCHEMA_TYPE != Settings::FINAL)
        {
            _outputChunkIterators[i]->setPosition(_outputPosition);
            _outputChunkIterators[i]->writeItem(_curHash);
            i++;
        }
        _outputChunkIterators[i]->setPosition(_outputPosition);
        _outputChunkIterators[i]->writeItem(_curGroup);
        i++;
        _outputChunkIterators[i]->setPosition(_outputPosition);
        if(SCHEMA_TYPE == Settings::FINAL)
        {
            Value result;
            _aggregate->finalResult(result, _curState);
            _outputChunkIterators[i]->writeItem(result);
        }
        else
        {
            _outputChunkIterators[i]->writeItem(_curState);
        }
        ++_outputValueNo;
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
        shared_ptr<Array> result = _output;
        _output.reset();
        return result;
    }
};

class ArrayCursor
{
private:
    shared_ptr<Array> _input;
    size_t const _nAttrs;
    vector <Value const *> _currentCell;
    bool _end;
    vector<shared_ptr<ConstArrayIterator> > _inputArrayIters;
    vector<shared_ptr<ConstChunkIterator> > _inputChunkIters;

public:
    ArrayCursor (shared_ptr<Array> const& input):
        _input(input),
        _nAttrs(input->getArrayDesc().getAttributes(true).size()),
        _currentCell(_nAttrs, 0),
        _end(false),
        _inputArrayIters(_nAttrs, 0),
        _inputChunkIters(_nAttrs, 0)
    {
        for(size_t i =0; i<_nAttrs; ++i)
        {
            _inputArrayIters[i] = _input->getConstIterator(i);
        }
        if (_inputArrayIters[0]->end())
        {
            _end=true;
        }
        else
        {
            advance();
        }
    }

    bool end() const
    {
        return _end;
    }

    size_t nAttrs() const
    {
        return _nAttrs;
    }

    void advance()
    {
        if(_end)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal error: iterating past end of cursor";
        }
        if (_inputChunkIters[0] == 0) //1st time!
        {
            for(size_t i =0; i<_nAttrs; ++i)
            {
                _inputChunkIters[i] = _inputArrayIters[i]->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS | ConstChunkIterator::IGNORE_EMPTY_CELLS);
            }
        }
        else if (!_inputChunkIters[0]->end()) //not first time!
        {
            for(size_t i =0; i<_nAttrs; ++i)
            {
                ++(*_inputChunkIters[i]);
            }
        }
        while(_inputChunkIters[0]->end())
        {
            for(size_t i =0; i<_nAttrs; ++i)
            {
                ++(*_inputArrayIters[i]);
            }
            if(_inputArrayIters[0]->end())
            {
                _end = true;
                return;
            }
            for(size_t i =0; i<_nAttrs; ++i)
            {
                _inputChunkIters[i] = _inputArrayIters[i]->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS | ConstChunkIterator::IGNORE_EMPTY_CELLS);
            }
        }
        for(size_t i =0; i<_nAttrs; ++i)
        {
            _currentCell[i] = &(_inputChunkIters[i]->getItem());
        }
    }

    vector <Value const *> const& getCell()
    {
        return _currentCell;
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

    shared_ptr<Array> flatSort(shared_ptr<Array> & input, shared_ptr<Query>& query, Settings& settings)
    {
        SortingAttributeInfos sortingAttributeInfos(2);
        sortingAttributeInfos[0].columnNo = 0;
        sortingAttributeInfos[0].ascent = true;
        sortingAttributeInfos[1].columnNo = 1;
        sortingAttributeInfos[1].ascent = true;
        SortArray sorter(input->getArrayDesc(), _arena, false, settings.getSpilloverChunkSize());
        shared_ptr<TupleComparator> tcomp(make_shared<TupleComparator>(sortingAttributeInfos, input->getArrayDesc()));
        return sorter.getSortedArray(input, query, tcomp);
    }

    shared_ptr<Array> localCondense(shared_ptr<Array>& inputArray, shared_ptr<Query>& query, Settings& settings)
    {
        AttributeID const groupAttribute = settings.getGroupAttributeId();
        AttributeID const aggregatedAttribute = settings.getInputAttributeId();
        AttributeComparator comparator( settings.getGroupAttributeType());
        ArenaPtr operatorArena = this->getArena();
        ArenaPtr hashArena(newArena(Options("").resetting(true).pagesize(8 * 1024 * 1204).parent(operatorArena)));
        AggregateHashTable aht(comparator, hashArena);
        AggregatePtr agg = settings.cloneAggregate();
        shared_ptr<ConstArrayIterator> gaiter(inputArray->getConstIterator(settings.getGroupAttributeId()));
        shared_ptr<ConstArrayIterator> iaiter(inputArray->getConstIterator(settings.getInputAttributeId()));
        shared_ptr<ConstChunkIterator> gciter, iciter;
        size_t const maxTableSize = 150*1024*1024;
        DoubleFloatOther dfo = getDoubleFloatOther(settings.getGroupAttributeType());
        bool spilloverSorted = true;
        MergeWriter<Settings::SPILL> flatWriter (settings, query);
        MergeWriter<Settings::MERGE> flatCondensed(settings, query);
        while(!gaiter->end())
        {
            gciter=gaiter->getChunk().getConstIterator();
            iciter=iaiter->getChunk().getConstIterator();
            while(!gciter->end())
            {
                uint64_t hash;
                Value const& group = gciter->getItem();
                if(group.isNull() || isNan(group, dfo))
                {
                    ++(*gciter);
                    ++(*iciter);
                    continue;
                }
                Value const& input = iciter->getItem();
                if(aht.usedBytes() < maxTableSize)
                {
                    aht.insert(group, input, agg);
                }
                else
                {
                    if(!aht.contains(group, hash))
                    {
                        if(settings.inputSorted())
                        {
                            flatCondensed.writeValue(hash, group, input);
                        }
                        else
                        {
                            flatWriter.writeValue(hash, group, input);
                        }
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
        gaiter.reset();
        iaiter.reset();
        gciter.reset();
        iciter.reset();
        shared_ptr<Array> arr = settings.inputSorted() ? flatCondensed.finalize() : flatWriter.finalize();
        arr = flatSort(arr, query, settings);
        aht.sortKeys();
        shared_ptr<ConstArrayIterator> haiter(arr->getConstIterator(0));
        gaiter = arr->getConstIterator(1);
        iaiter = arr->getConstIterator(2);
        shared_ptr<ConstChunkIterator> hciter;
        aht.logStuff();
        AggregateHashTable::const_iterator ahtIter = aht.getIterator();
        MergeWriter<Settings::MERGE> mergeWriter(settings, query);
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
                if(settings.inputSorted())
                {
                    mergeWriter.writeState(hash,group,input);
                }
                else
                {
                    mergeWriter.writeValue(hash,group,input);
                }
                ++(*hciter);
                ++(*gciter);
                ++(*iciter);
            }
            ++(*haiter);
            ++(*gaiter);
            ++(*iaiter);
        }
        hciter.reset();
        gciter.reset();
        iciter.reset();
        haiter.reset();
        gaiter.reset();
        iaiter.reset();
        while(!ahtIter.end())
        {
            mergeWriter.writeState(ahtIter.getCurrentHash(), ahtIter.getCurrentGroup(), ahtIter.getCurrentState());
            ahtIter.next();
        }
        return mergeWriter.finalize();
    }

    shared_ptr<Array> globalMerge(shared_ptr<Array>& inputArray, shared_ptr<Query>& query, Settings& settings)
    {
        inputArray = redistributeToRandomAccess(inputArray, query, psByRow, ALL_INSTANCE_MASK, std::shared_ptr<CoordinateTranslator>(), 0, std::shared_ptr<PartitioningSchemaData>());
        AttributeComparator comparator( settings.getGroupAttributeType() );
        MergeWriter<Settings::FINAL> output(settings, query, _schema.getName());
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
            haiters[inst] = inputArray->getConstIterator(0);
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
                gaiters[inst] = inputArray->getConstIterator(1);
                gaiters[inst]->setPosition(positions[inst]);
                vaiters[inst] = inputArray->getConstIterator(2);
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
                        positions[inst][2] = positions[inst][2] + settings.getMergeChunkSize();
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

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        Settings settings(inputArrays[0]->getArrayDesc(), _parameters, true, query);
        shared_ptr<Array> array = inputArrays[0];
        array = localCondense(array, query, settings);
        array = globalMerge(array, query, settings);
        return array;

    }
};
REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalGroupedAggregate, "grouped_aggregate", "physical_grouped_aggregate");
} //namespace scidb
