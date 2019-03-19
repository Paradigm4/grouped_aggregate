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


#include <query/PhysicalOperator.h>
#include <array/Metadata.h>
#include <system/Cluster.h>
#include <query/Query.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <log4cxx/logger.h>
#include <network/NetworkMessage.h>
#include <array/RLE.h>
#include <array/MemArray.h>
#include <array/SortArray.h>

#include <array/ArrayIterator.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory>
#include <cstddef>

#include "AggregateHashTable.h"
#include "GroupedAggregateSettings.h"

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
    size_t const _groupSize;
    size_t const _numAggs;
    size_t const _chunkSize;
    size_t const _numInstances;
    InstanceID const _myInstanceId;
    vector<uint32_t> _hashBreaks;
    size_t _currentBreak;
    shared_ptr<Query> _query;
    Settings& _settings;
    Coordinates _outputPosition;
    Coordinate& _outputValueNo;
    shared_ptr<ArrayIterator> _hashArrayIterator;
    shared_ptr<ChunkIterator> _hashChunkIterator;
    vector<shared_ptr<ArrayIterator> > _groupArrayIterators;
    vector<shared_ptr<ChunkIterator> > _groupChunkIterators;
    vector<shared_ptr<ArrayIterator> >_itemArrayIterators;
    vector<shared_ptr<ChunkIterator> >_itemChunkIterators;
    Value            _curHash;
    vector<Value>    _curGroup;
    vector<Value>    _curStates;

public:
    MergeWriter(Settings& settings, shared_ptr<Query> const& query, string const name = ""):
        _output(make_shared<MemArray>(settings.makeSchema(query,SCHEMA_TYPE, name), query)),
        _groupSize(settings.getGroupSize()),
        _numAggs(settings.getNumAggs()),
        _chunkSize(_output->getArrayDesc().getDimensions()[_output->getArrayDesc().getDimensions().size()-1].getChunkInterval()),
        _numInstances(query->getInstancesCount()),
        _myInstanceId(query->getInstanceID()),
        _hashBreaks(_numInstances-1,0),
        _query(query),
        _settings(settings),
        _outputPosition( SCHEMA_TYPE == Settings::SPILL ? 1 :
                         SCHEMA_TYPE== Settings::MERGE ?  3 :
                                                          2 , 0),
        _outputValueNo(  SCHEMA_TYPE == Settings::SPILL ? _outputPosition[0] :
                         SCHEMA_TYPE == Settings::MERGE ? _outputPosition[2] :
                                                          _outputPosition[1]),
        _hashArrayIterator(NULL),
        _hashChunkIterator(NULL),
        _groupArrayIterators(_groupSize, NULL),
        _groupChunkIterators(_groupSize, NULL),
        _itemArrayIterators(_numAggs, NULL),
        _itemChunkIterators(_numAggs, NULL),
        _curGroup(_groupSize),
        _curStates(_numAggs)
    {
        _curHash.setNull(0);
        for(size_t i=0; i<_groupSize; ++i)
        {
            _curGroup[i].setNull(0);
        }
        for(size_t i=0; i<_numAggs; ++i)
        {
            _curStates[i].setNull(0);
        }
        uint32_t break_interval = _settings.getNumHashBuckets() / _numInstances; //XXX:CAN'T DO EASY ROUNDOFF
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
        AttributeID i = 0;
        if(SCHEMA_TYPE != Settings::FINAL)
        {
            auto aidIter = _output->getArrayDesc().getAttributes().find(i);
            _hashArrayIterator = _output->getIterator(*aidIter);
            ++i;
        }
        for(size_t j =0; j<_groupSize; ++j)
        {
            auto aidIter = _output->getArrayDesc().getAttributes().find(i);
            _groupArrayIterators[j] = _output->getIterator(*aidIter);
            ++i;
        }
        for(size_t j=0; j<_numAggs; ++j)
        {
            auto aidIter = _output->getArrayDesc().getAttributes().find(i);
            _itemArrayIterators[j] = _output->getIterator(*aidIter);
            ++i;
        }
    }

private:
    void copyGroup( vector<Value const*> const& group)
    {
        for(size_t i =0; i<_groupSize; ++i)
        {
            _curGroup[i] = *(group[i]);
        }
    }

public:
    void writeValue (uint32_t const hash, vector<Value const*> const& group, vector<Value const*> const& inputs)
    {
        Value buf;
        buf.setUint32(hash);
        writeValue(buf, group, inputs);
    }

    void writeValue (Value const& hash, vector<Value const*> const& group, vector<Value const*> const& inputs)
    {
        if(SCHEMA_TYPE == Settings::SPILL )
        {
            if(_curHash.getMissingReason() != 0)
            {
                writeCurrent();
            }
            _curHash = hash;
            copyGroup(group);
            for(size_t i =0; i<_numAggs; ++i)
            {
                _curStates[i] = *(inputs[i]);
            }
        }
        else
        {
            if(_curHash.getMissingReason() == 0 || _curHash.getUint32() != hash.getUint32() || !_settings.groupEqual(&(_curGroup[0]), group))
            {
                if(_curHash.getMissingReason() != 0)
                {
                    writeCurrent();
                }
                _curHash = hash;
                copyGroup(group);
                _settings.aggInitState(&(_curStates[0]));
            }
            _settings.aggAccumulate(&(_curStates[0]), inputs);
        }
    }

    void writeState (uint32_t const hash, vector<Value const*> const& group, vector<Value const*> const& states)
    {
        Value buf;
        buf.setUint32(hash);
        writeState(buf, group, states);
    }

    void writeState (Value const& hash, vector<Value const*> const& group, vector<Value const*> const& states)
    {
        if(SCHEMA_TYPE == Settings::SPILL)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "don't call writeState on a SPILL writer";
        }
        else
        {
            if(_curHash.getMissingReason() == 0 || _curHash.getUint32() != hash.getUint32() || !_settings.groupEqual(&(_curGroup[0]), group))
            {
                if(_curHash.getMissingReason() != 0)
                {
                    writeCurrent();
                }
                _curHash = hash;
                copyGroup(group);
                _settings.aggInitState(&(_curStates[0]));
            }
            _settings.aggMerge(&(_curStates[0]), states);
        }
    }

private:
    void writeCurrent()
    {
        //gonna do a write, then!
        while( SCHEMA_TYPE == Settings::MERGE && _currentBreak < _numInstances - 1 && _curHash.getUint32() > _hashBreaks[_currentBreak] )
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
            size_t i = 0;
            if(SCHEMA_TYPE != Settings::FINAL)
            {
                if(_hashChunkIterator.get())
                {
                    _hashChunkIterator->flush();
                }
                _hashChunkIterator = _hashArrayIterator -> newChunk(_outputPosition).getIterator(_query,
                                                i == 0 ? ChunkIterator::SEQUENTIAL_WRITE : ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
                ++i;
            }
            for(size_t j =0; j<_groupSize; ++j)
            {
                if(_groupChunkIterators[j].get())
                {
                    _groupChunkIterators[j]->flush();
                }
                _groupChunkIterators[j] = _groupArrayIterators[j]->newChunk(_outputPosition).getIterator(_query,
                                                i == 0 ? ChunkIterator::SEQUENTIAL_WRITE : ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
                ++i;
            }
            for(size_t j =0; j<_numAggs; ++j)
            {
                if(_itemChunkIterators[j].get())
                {
                    _itemChunkIterators[j]->flush();
                }
                _itemChunkIterators[j] = _itemArrayIterators[j] -> newChunk(_outputPosition).getIterator(_query,
                                               i == 0 ? ChunkIterator::SEQUENTIAL_WRITE : ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
                ++i;
            }
        }
        if(SCHEMA_TYPE != Settings::FINAL)
        {
            _hashChunkIterator->setPosition(_outputPosition);
            _hashChunkIterator->writeItem(_curHash);
        }
        for(size_t j =0; j<_groupSize; ++j)
        {
            _groupChunkIterators[j]->setPosition(_outputPosition);
            _groupChunkIterators[j]->writeItem(_curGroup[j]);
        }
        if(SCHEMA_TYPE == Settings::FINAL)
        {
            vector<Value> result(_numAggs);
            _settings.aggFinal(&(result[0]), &(_curStates[0]));
            for (size_t j=0; j<_numAggs; ++j)
            {
                _itemChunkIterators[j]->setPosition(_outputPosition);
                _itemChunkIterators[j]->writeItem(result[j]);
            }
        }
        else
        {
            for (size_t j=0; j<_numAggs; ++j)
            {
                _itemChunkIterators[j]->setPosition(_outputPosition);
                _itemChunkIterators[j]->writeItem(_curStates[j]);
            }
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
        if(SCHEMA_TYPE != Settings::FINAL && _hashChunkIterator.get())
        {
            _hashChunkIterator->flush();
        }
        _hashChunkIterator.reset();
        _hashArrayIterator.reset();
        for(size_t j =0; j<_groupSize; ++j)
        {
            if(_groupChunkIterators[j].get())
            {
                _groupChunkIterators[j]->flush();
            }
            _groupChunkIterators[j].reset();
            _groupArrayIterators[j].reset();
        }
        for(size_t j =0; j<_numAggs; ++j)
        {
            if(_itemChunkIterators[j].get())
            {
                _itemChunkIterators[j]->flush();
            }
            _itemChunkIterators[j].reset();
            _itemArrayIterators[j].reset();
        }
        shared_ptr<Array> result = _output;
        _output.reset();
        return result;
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

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
      // Distribution is undefined.
      SCIDB_ASSERT(_schema.getDistribution()->getDistType()==dtUndefined);
      return _schema.getDistribution()->getDistType();
    }

    /// @see PhysicalOperator
    virtual RedistributeContext getOutputDistribution(vector<RedistributeContext> const& inputDistributions,
                                                      vector<ArrayDesc> const& inputSchemas) const override
    {
      assertConsistency(inputSchemas[0], inputDistributions[0]);

      // Distribution is undefined.
      SCIDB_ASSERT(_schema.getDistribution()->getDistType()==dtUndefined);
      _schema.setResidency(inputDistributions[0].getArrayResidency());

      return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    shared_ptr<Array> flatSort(shared_ptr<Array> & input, shared_ptr<Query>& query, Settings& settings)
    {
        SortingAttributeInfos sortingAttributeInfos(settings.getGroupSize() + 1);
        sortingAttributeInfos[0].columnNo = 0;
        sortingAttributeInfos[0].ascent = true;
        for(size_t g=0; g<settings.getGroupSize(); ++g)
        {
            sortingAttributeInfos[g+1].columnNo = g+1;
            sortingAttributeInfos[g+1].ascent = true;
        }
        SortArray sorter(input->getArrayDesc(), _arena); //, false, settings.getSpilloverChunkSize());
        shared_ptr<TupleComparator> tcomp(make_shared<TupleComparator>(sortingAttributeInfos, input->getArrayDesc()));
        return sorter.getSortedArray(input, query, shared_from_this(), tcomp, NULL);
    }

    shared_ptr<Array> localCondense(shared_ptr<Array>& inputArray, shared_ptr<Query>& query, Settings& settings)
    {
        ArenaPtr operatorArena = this->getArena();
        ArenaPtr hashArena(newArena(Options("").resetting(true).threading(false).pagesize(8 * 1024 * 1204).parent(operatorArena)));
        AggregateHashTable aht(settings, hashArena);
        size_t const groupSize = settings.getGroupSize();
        vector<shared_ptr<ConstArrayIterator> > gaiters(groupSize,NULL);
        vector<shared_ptr<ConstChunkIterator> > gciters(groupSize,NULL);
        vector<int64_t> const& groupIds = settings.getGroupIds();
        vector<Value> coords(groupSize);
        vector<Value const*> group(groupSize, NULL);
        for(size_t g=0; g<groupSize; ++g)
        {
            if(!settings.isGroupOnAttribute(g))
            {
                group[g] = &(coords[g]);
            }
            else
            {
                auto aidIter = inputArray->getArrayDesc().getAttributes().find(groupIds[g]);
                gaiters[g] = inputArray->getConstIterator( *aidIter );
            }
        }
        if(gaiters[0].get() == 0)
        {   //TODO: also covers the case when the user wants to group by dimensions only
            AttributeID lastAttrId = inputArray->getArrayDesc().getAttributes().size()-1;
            auto aidIter = inputArray->getArrayDesc().getAttributes().find(lastAttrId);
            gaiters[0] = inputArray->getConstIterator(*aidIter);
        }
        size_t const numAggs = settings.getNumAggs();
        vector<shared_ptr<ConstArrayIterator> > iaiters(numAggs, NULL);
        vector<shared_ptr<ConstChunkIterator> > iciters(numAggs, NULL);
        vector<Value const*> input(numAggs, NULL);
        for(size_t a=0; a<numAggs; ++a)
        {
            auto aidIter = inputArray->getArrayDesc().getAttributes().find(settings.getInputAttributeIds()[a]);
            iaiters[a] = inputArray->getConstIterator( *aidIter );
        }
        size_t const maxTableSize = settings.getMaxTableSize();
        MergeWriter<Settings::SPILL> flatWriter (settings, query);
        MergeWriter<Settings::MERGE> flatCondensed(settings, query);
        while(!gaiters[0]->end())
        {
            for(size_t g=0; g<groupSize; ++g)
            {
                if(gaiters[g].get())
                {
                    gciters[g] = gaiters[g]->getChunk().getConstIterator();
                }
            }
            for(size_t a=0; a<numAggs; ++a)
            {
                iciters[a] = iaiters[a]->getChunk().getConstIterator();
            }
            while(!gciters[0]->end())
            {
                Coordinates const& position = gciters[0]->getPosition();
                for(size_t g=0; g<groupSize; ++g)
                {
                    if(settings.isGroupOnAttribute(g))
                    {
                        group[g] = &(gciters[g]->getItem());
                    }
                    else
                    {
                        coords[g].setInt64( position[ groupIds[g] ] );
                    }
                }
                if(!settings.groupValid(group))
                {
                    for(size_t g=0; g<groupSize; ++g)
                    {
                        if(gaiters[g].get())
                        {
                            ++(*(gciters[g]));
                        }
                    }
                    for(size_t a=0; a<numAggs; ++a)
                    {
                        ++(*(iciters[a]));
                    }
                    continue;
                }
                uint32_t hash;
                for(size_t a=0; a<numAggs; ++a)
                {
                    input[a] = &(iciters[a]->getItem());
                }
                if(aht.usedBytes() < maxTableSize)
                {
                    aht.insert(group, input);
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
                        aht.insert(group, input);
                    }
                }
                for(size_t g=0; g<groupSize; ++g)
                {
                    if(gaiters[g].get())
                    {
                        ++(*(gciters[g]));
                    }
                }
                for(size_t a=0; a<numAggs; ++a)
                {
                    ++(*(iciters[a]));
                }
            }
            for(size_t g=0; g<groupSize; ++g)
            {
                if(gaiters[g].get())
                {
                    ++(*(gaiters[g]));
                }
            }
            for(size_t a=0; a<numAggs; ++a)
            {
                ++(*(iaiters[a]));
            }
        }
        for(size_t g = 0; g<groupSize; ++g)
        {
            gciters[g].reset();
            gaiters[g].reset();
        }
        for(size_t a=0; a<numAggs; ++a)
        {
            iciters[a].reset();
            iaiters[a].reset();
        }
        shared_ptr<Array> arr = settings.inputSorted() ? flatCondensed.finalize() : flatWriter.finalize();
        arr = flatSort(arr, query, settings);
        aht.logStuff();
        auto aidIter = inputArray->getArrayDesc().getAttributes().find(0);
        shared_ptr<ConstArrayIterator> haiter(arr->getConstIterator(*aidIter));
        for(size_t g = 0; g<groupSize; ++g)
        {
            auto aidIter = inputArray->getArrayDesc().getAttributes().find(g+1);
            gaiters[g] = arr->getConstIterator(*aidIter);
        }
        for(size_t a = 0; a<numAggs; ++a)
        {
            auto aidIter = inputArray->getArrayDesc().getAttributes().find(a + groupSize + 1);
            iaiters[a] = arr->getConstIterator(*aidIter);
        }
        shared_ptr<ConstChunkIterator> hciter;
        AggregateHashTable::const_iterator ahtIter = aht.getIterator();
        MergeWriter<Settings::MERGE> mergeWriter(settings, query);
        while(!haiter->end())
        {
            hciter = haiter->getChunk().getConstIterator();
            for(size_t g = 0; g<groupSize; ++g)
            {
                gciters[g] = gaiters[g]->getChunk().getConstIterator();
            }
            for(size_t a = 0; a<numAggs; ++a)
            {
                iciters[a] = iaiters[a]->getChunk().getConstIterator();
            }
            while(!hciter->end())
            {
                Value const& hash  = hciter->getItem();
                for(size_t g = 0; g<groupSize; ++g)
                {
                    group[g] = &(gciters[g]->getItem());
                }
                for(size_t a = 0; a<numAggs; ++a)
                {
                    input[a] = &(iciters[a]->getItem());
                }
                while(!ahtIter.end() && (ahtIter.getCurrentHash() < hash.getUint32() ||
                                        (ahtIter.getCurrentHash() == hash.getUint32() && settings.groupLess(ahtIter.getCurrentGroup(), group))))
                {
                    mergeWriter.writeState(ahtIter.getCurrentHash(), ahtIter.getGroupVector(), ahtIter.getStateVector());
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
                for(size_t g = 0; g<groupSize; ++g)
                {
                    ++(*(gciters[g]));
                }
                for(size_t a = 0; a<numAggs; ++a)
                {
                    ++(*(iciters[a]));
                }
            }
            ++(*haiter);
            for(size_t g = 0; g<groupSize; ++g)
            {
                ++(*(gaiters[g]));
            }
            for(size_t a = 0; a<numAggs; ++a)
            {
                ++(*(iaiters[a]));
            }
        }
        while(!ahtIter.end())
        {
            mergeWriter.writeState(ahtIter.getCurrentHash(), ahtIter.getGroupVector(), ahtIter.getStateVector());
            ahtIter.next();
        }
        hciter.reset();
        haiter.reset();
        for(size_t g = 0; g<groupSize; ++g)
        {
            gciters[g].reset();
            gaiters[g].reset();
        }
        for(size_t a = 0; a<numAggs; ++a)
        {
            iciters[a].reset();
            iaiters[a].reset();
        }
        return mergeWriter.finalize();
    }

    shared_ptr<Array> globalMerge(shared_ptr<Array>& inputArray, shared_ptr<Query>& query, Settings& settings)
    {

    	//inputArray = redistributeToRandomAccess(inputArray, query, dtRowCyclic, ALL_INSTANCE_MASK, std::shared_ptr<CoordinateTranslator>(), 0, std::shared_ptr<PartitioningSchemaData>());
    	//inputArray = redistributeToRandomAccess(inputArray,createDistribution(dtRowCyclic),query->getDefaultArrayResidency(), query, true);
    	inputArray = redistributeToRandomAccess(inputArray,createDistribution(dtRowCyclic),query->getDefaultArrayResidency(), query, shared_from_this());

        MergeWriter<Settings::FINAL> output(settings, query, _schema.getName());
        size_t const numInstances = query->getInstancesCount();
        size_t const groupSize    = settings.getGroupSize();
        size_t const numAggs      = settings.getNumAggs();
        vector<shared_ptr<ConstArrayIterator> > haiters(numInstances);
        vector<shared_ptr<ConstChunkIterator> > hciters(numInstances);
        vector<shared_ptr<ConstArrayIterator> > gaiters(numInstances * groupSize);
        vector<shared_ptr<ConstChunkIterator> > gciters(numInstances * groupSize);
        vector<shared_ptr<ConstArrayIterator> > vaiters(numInstances * numAggs);
        vector<shared_ptr<ConstChunkIterator> > vciters(numInstances * numAggs);
        vector<Coordinates > positions(numInstances);
        size_t numClosed = 0;
        for(size_t inst =0; inst<numInstances; ++inst)
        {
            positions[inst].resize(3);
            positions[inst][0] = query->getInstanceID();
            positions[inst][1] = inst;
            positions[inst][2] = 0;
            auto aidIter = inputArray->getArrayDesc().getAttributes().find(0);
            haiters[inst] = inputArray->getConstIterator(*aidIter);
            if(!haiters[inst]->setPosition(positions[inst]))
            {
                haiters[inst].reset();
                hciters[inst].reset();
                for(size_t g=0; g<groupSize; ++g)
                {
                    gaiters[inst * groupSize + g].reset();
                    gciters[inst * groupSize + g].reset();
                }
                for(size_t a=0; a<numAggs; ++a)
                {
                    vaiters[inst * numAggs + a].reset();
                    vciters[inst * numAggs + a].reset();
                }
                numClosed++;
            }
            else
            {
                hciters[inst] = haiters[inst]->getChunk().getConstIterator();
                for(size_t g =0; g<groupSize; ++g)
                {
                    auto aidIter = inputArray->getArrayDesc().getAttributes().find(1 + g);
                    gaiters[inst * groupSize + g] = inputArray->getConstIterator(*aidIter);
                    gaiters[inst * groupSize + g]->setPosition(positions[inst]);
                    gciters[inst * groupSize + g] = gaiters[inst * groupSize + g]->getChunk().getConstIterator();
                }
                for(size_t a=0; a<numAggs; ++a)
                {
                    auto aidIter = inputArray->getArrayDesc().getAttributes().find(1 + groupSize + a);
                    vaiters[inst * numAggs + a] = inputArray->getConstIterator( *aidIter );
                    vaiters[inst * numAggs + a]->setPosition(positions[inst]);
                    vciters[inst * numAggs + a] = vaiters[inst * numAggs + a]->getChunk().getConstIterator();
                }
            }
        }
        vector<Value const*> minGroup(groupSize, NULL);
        vector<Value const*> curGroup(groupSize, NULL);
        vector<Value const*> curState(numAggs,   NULL);
        while(numClosed < numInstances)
        {
            bool minHashSet = false;
            uint32_t minHash=0;
            for(size_t inst=0; inst<numInstances; ++inst)
            {
                if(hciters[inst] == 0)
                {
                    continue;
                }
                uint32_t hash    = hciters[inst]->getItem().getUint32();
                for(size_t g=0; g<groupSize; ++g)
                {
                    curGroup[g] = &(gciters[inst * groupSize + g]->getItem());
                }
                if(!minHashSet || (hash < minHash || (hash == minHash && settings.groupLess(curGroup, minGroup))))
                {
                    minHash = hash;
                    minGroup = curGroup;
                    minHashSet = true;
                }
            }
            vector<size_t> toAdvance;
            for(size_t inst=0; inst<numInstances; ++inst)
            {
                if(hciters[inst] == 0)
                {
                    continue;
                }
                uint32_t hash    = hciters[inst]->getItem().getUint32();
                for(size_t g=0; g<groupSize; ++g)
                {
                    curGroup[g] = &(gciters[inst * groupSize + g]->getItem());
                }
                if(hash == minHash && settings.groupEqual(curGroup, minGroup))
                {
                    toAdvance.push_back(inst);
                }
            }
            for(size_t i =0; i<toAdvance.size(); ++i)
            {
                size_t const inst = toAdvance[i];
                uint32_t hash    = hciters[inst]->getItem().getUint32();
                for(size_t g=0; g<groupSize; ++g)
                {
                    curGroup[g] = &(gciters[inst * groupSize + g]->getItem());
                }
                for(size_t a=0; a<numAggs; ++a)
                {
                    curState[a] = &(vciters[inst * numAggs + a]->getItem());
                }
                output.writeState(hash, curGroup, curState);
                ++(*hciters[inst]);
                for(size_t g=0; g<groupSize; ++g)
                {
                    ++(*gciters[inst * groupSize + g]);
                }
                for(size_t a=0; a<numAggs; ++a)
                {
                    ++(*vciters[inst * numAggs + a]);
                }
                if(hciters[inst]->end())
                {
                    positions[inst][2] = positions[inst][2] + settings.getMergeChunkSize();
                    bool sp = haiters[inst]->setPosition(positions[inst]);
                    if(!sp)
                    {
                        haiters[inst].reset();
                        hciters[inst].reset();
                        for(size_t g=0; g<groupSize; ++g)
                        {
                            gaiters[inst * groupSize + g].reset();
                            gciters[inst * groupSize + g].reset();
                        }
                        for(size_t a=0; a<numAggs; ++a)
                        {
                            vaiters[inst * numAggs + a].reset();
                            vciters[inst * numAggs + a].reset();
                        }
                        numClosed++;
                    }
                    else
                    {
                        hciters[inst] = haiters[inst]->getChunk().getConstIterator();
                        for(size_t g=0; g<groupSize; ++g)
                        {
                            gaiters[inst * groupSize + g]->setPosition(positions[inst]);
                            gciters[inst * groupSize + g] = gaiters[inst * groupSize + g]->getChunk().getConstIterator();
                        }
                        for(size_t a=0; a<numAggs; ++a)
                        {
                            vaiters[inst * numAggs + a]->setPosition(positions[inst]);
                            vciters[inst * numAggs + a] = vaiters[inst * numAggs + a]->getChunk().getConstIterator();
                        }
                    }
                }
            }
        }
        return output.finalize();
    }

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        Settings settings(inputArrays[0]->getArrayDesc(), _parameters, _kwParameters, query);
        shared_ptr<Array> array = inputArrays[0];
        array = localCondense(array, query, settings);
        array = globalMerge(array, query, settings);
        return array;
    }
};
REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalGroupedAggregate, "grouped_aggregate", "physical_grouped_aggregate");
} //namespace scidb
