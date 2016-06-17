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

#ifndef HASH_TABLE_UTILITIES
#define HASH_TABLE_UTILITIES

#include <query/Operator.h>
#include <query/AttributeComparator.h>
#include <util/arena/Set.h>
#include <util/arena/Map.h>
#include <util/arena/Vector.h>
#include <util/arena/List.h>
#include <util/Arena.h>
#include <array/SortArray.h>
#include <array/TupleArray.h>
#include "GroupedAggregateSettings.h"

namespace scidb
{
namespace grouped_aggregate
{

using scidb::arena::Options;
using scidb::arena::ArenaPtr;
using scidb::arena::newArena;
using scidb::SortArray;
using std::shared_ptr;
using std::dynamic_pointer_cast;
using grouped_aggregate::Settings;

// MurmurHash2, 64-bit versions, by Austin Appleby
// From https://sites.google.com/site/murmurhash/
// MIT license
uint64_t mh64a ( const void * key, int len, uint32_t const seed = 0x5C1DB123 )
{
    if (key == 0 || len == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "bad murmurhash call";
    }
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ len;
    const uint64_t * data = (const uint64_t *)key;
    const uint64_t * end = data + (len/8);
    while(data != end)
    {
            uint64_t k = *data++;
            k *= m;
            k ^= k >> r;
            k *= m;
            h ^= k;
            h *= m;
    }
    const unsigned char * data2 = (const unsigned char*)data;
    switch(len & 7)
    {
    case 7: h ^= uint64_t(data2[6]) << 48;
    case 6: h ^= uint64_t(data2[5]) << 40;
    case 5: h ^= uint64_t(data2[4]) << 32;
    case 4: h ^= uint64_t(data2[3]) << 24;
    case 3: h ^= uint64_t(data2[2]) << 16;
    case 2: h ^= uint64_t(data2[1]) << 8;
    case 1: h ^= uint64_t(data2[0]);
            h *= m;
    };
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

uint64_t hashGroup(std::vector<Value const*> const& group, size_t const groupSize)
{
    size_t totalSize = 0;
    for(size_t i =0; i<groupSize; ++i)
    {
        totalSize += group[i]->size();
    }
    static std::vector<char> buf (64);
    if(buf.size() < totalSize)
    {
        buf.resize(totalSize);
    }
    char* ch = &buf[0];
    for(size_t i =0; i<groupSize; ++i)
    {
        memcpy(ch, group[i]->data(), group[i]->size());
        ch += group[i]->size();
    }
    return mh64a(&buf[0], totalSize);
}

static void EXCEPTION_ASSERT(bool cond)
{
    if (! cond)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
    }
}

struct HashTableEntry
{
    uint64_t hash;
    size_t   idx;

    HashTableEntry(uint64_t const hash, size_t const idx):
        hash(hash), idx(idx)
    {}
};

class AggregateHashTable
{
private:
    Settings&                                _settings;
    ArenaPtr                                 _arena;
    size_t const                             _groupSize;
    size_t const                             _numAggs;
    size_t const                             _numHashBuckets;
    mgd::vector<uint64_t>                    _hashes;
    mgd::vector< mgd::list<HashTableEntry> > _buckets;
    mgd::vector< Value >                     _values;
    mgd::list<HashTableEntry>::iterator      _iter;
    Value const*                             _lastGroup;
    Value *                                  _lastState;
    ssize_t                                  _largeValueMemory;
    size_t                                   _numGroups;

    void accumulateStates(Value* states, std::vector<Value const*> const& input)
    {
       size_t initialMem = 0;
       for(size_t i =0; i<_numAggs; ++i)
       {
           if(states[i].isLarge())
           {
               initialMem += states[i].size();
           }
       }
       _settings.aggAccumulate(states, input);
       size_t finalMem = 0;
       for(size_t i =0; i<_numAggs; ++i)
       {
          if(states[i].isLarge())
          {
              finalMem += states[i].size();
          }
       }
       _largeValueMemory += (finalMem - initialMem);
    }

    size_t addNewGroup(std::vector<Value const*> const& group, std::vector<Value const*> const& input)
    {
        size_t idx = _values.size();
        for(size_t i=0; i<_groupSize; ++i)
        {
            Value const& groupItem = *(group[i]);
            if(groupItem.isLarge())
            {
                _largeValueMemory += groupItem.size();
            }
            _values.push_back(groupItem);
        }
        _values.resize( _values.size() + _numAggs );
        Value* statePtr = &(_values[idx + _groupSize]);
        _settings.aggInitState(statePtr);
        accumulateStates(statePtr, input);
        return idx;
    }

    Value const* getGroup(size_t const idx) const
    {
        return &(_values[idx]);
    }

    Value* getStates(size_t const idx)
    {
        return &(_values[idx + _groupSize]);
    }

public:
    AggregateHashTable(Settings& settings, ArenaPtr const& arena):
        _settings(settings),
        _arena(arena),
        _groupSize(settings.getGroupSize()),
        _numAggs(settings.getNumAggs()),
        _numHashBuckets(settings.getNumHashBuckets()),
        _hashes(_arena, 0),
        _buckets(_arena, _numHashBuckets, mgd::list<HashTableEntry>(_arena)),
        _values(_arena, 0),
        _lastGroup(NULL),
        _lastState(NULL),
        _largeValueMemory(0),
        _numGroups(0)
    {}

    void insert(std::vector<Value const*> const& group, std::vector<Value const*> const& input)
    {
        if(_lastGroup != NULL && _settings.groupEqual(_lastGroup, group))
        {
            accumulateStates(_lastState, input);
            return;
        }
        uint64_t hash = hashGroup(group, _groupSize);
        mgd::list<HashTableEntry>& bucket = _buckets[hash % _numHashBuckets];
        _iter = bucket.begin();
        while( _iter != bucket.end() && _iter->hash < hash)
        {
            ++_iter;
        }
        Value const* storedGrp = NULL;
        Value* storedStates = NULL;
        bool hashExists  = false;
        bool equal       = false;
        while (_iter != bucket.end() && _iter->hash == hash)
        {
            hashExists = true;
            storedGrp = getGroup( _iter->idx);
            storedStates = getStates( _iter->idx);
            if(_settings.groupEqual(storedGrp, group))
            {
                equal = true;
                break;
            }
            else if( ! _settings.groupLess(storedGrp, group))
            {
                break;
            }
            ++_iter;
        }
        if( hashExists && equal )
        {
            accumulateStates(storedStates, input);
            _lastGroup = getGroup(_iter->idx);
            _lastState = getStates(_iter->idx);
        }
        else
        {
            if(!hashExists)
            {
                _hashes.push_back(hash);
            }
            size_t idx = addNewGroup(group, input);
            bucket.emplace(_iter, hash, idx);
            ++_numGroups;
            _lastGroup = getGroup(idx);
            _lastState = getStates(idx);
        }
    }

    /**
     * @param[out] hash computes the hash of group as a side-effect
     * @return true if the table contains the group, false otherwise
     */
    bool contains(std::vector<Value const*> const& group, uint64_t& hash) const
    {
        hash = hashGroup(group, _groupSize);
        mgd::list<HashTableEntry> const& bucket = _buckets[hash % _numHashBuckets];
        mgd::list<HashTableEntry>::const_iterator citer = bucket.begin();
        while(citer != bucket.end())
        {
            if(citer->hash == hash && _settings.groupEqual(getGroup(citer->idx), group))
            {
                return true;
            }
            if(citer->hash > hash)
            {
                return false;
            }
            ++citer;
        }
        return false;
    }

    /**
     * @return the total amount of bytes used by the structure
     */
    size_t usedBytes() const
    {
        if(_largeValueMemory < 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)<<" inconsistent state size overflow";
        }
        return _arena->allocated() + _largeValueMemory;
    }

    /**
     * Sort the hash keys in the table. A subsequent call to getIterator shall return an iterator
     * that will iterate over the data in the order of increasing hash, then increasing group.
     * If this is not called, the iterator will return over the hashes in arbitrary order.
     * This is a lot easier than dumping the thing into an array and sorting that.
     */
    void sortKeys()
    {
        std::sort(_hashes.begin(), _hashes.end());
    }

    class const_iterator
    {
    private:
        mgd::vector <Value> const& _values;
        mgd::vector <mgd::list<HashTableEntry> > const& _buckets;
        mgd::vector<size_t> const& _hashes;
        size_t const _groupSize;
        size_t const _numAggs;
        size_t const _numHashBuckets;
        mgd::vector<size_t>::const_iterator _hashIter;
        uint64_t _currHash;
        mgd::list<HashTableEntry> const* _bucket;
        mgd::list<HashTableEntry>::const_iterator _bucketIter;
        vector<Value const*> _groupResult;
        vector<Value const*> _aggStateResult;

    public:
        /**
         * To get one, call AggregateHashTable::getIterator
         */
        const_iterator(mgd::vector<Value> const& values,
                       mgd::vector <mgd::list<HashTableEntry> > const& buckets,
                       mgd::vector<size_t> const& hashes,
                       size_t const groupSize, size_t const numAggs, size_t const numHashBuckets):
          _values(values),
          _buckets(buckets),
          _hashes(hashes),
          _groupSize(groupSize),
          _numAggs(numAggs),
          _numHashBuckets(numHashBuckets),
          _groupResult(groupSize, NULL),
          _aggStateResult(numAggs, NULL)
        {
            restart();
        }

        /**
         * Set the iterator at the first hash in the table
         */
        void restart()
        {
            _hashIter = _hashes.begin();
            if(_hashIter != _hashes.end())
            {
                _currHash = (*_hashIter);
                _bucket = &(_buckets[_currHash % _numHashBuckets]);
                _bucketIter = _bucket->begin();
                while(_bucketIter->hash != _currHash)
                {
                    ++_bucketIter;
                }
            }
        }

        /**
         * @return true if the iterator has no more items, false otherwise
         */
        bool end() const
        {
            return _hashIter == _hashes.end();
        }

        /**
         * advance the iterator
         */
        void next()
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "iterating past end";
            }
            ++(_bucketIter);
            if (_bucketIter == _bucket->end() || _bucketIter->hash != _currHash)
            {
                ++(_hashIter);
                if(end())
                {
                    return;
                }
                _currHash = (*_hashIter);
                _bucket = &(_buckets[_currHash % _numHashBuckets]);
                _bucketIter = _bucket->begin();
                while(_bucketIter->hash != _currHash)
                {
                   ++_bucketIter;
                }
            }
        }

        //GETTERS
        uint64_t getCurrentHash() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return (*_hashIter);
        }

        Value const* getCurrentGroup() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return &(_values[_bucketIter->idx]);
        }

        vector<Value const*> const& getGroupVector()
        {
            Value const* g = getCurrentGroup();
            for(size_t i =0; i<_groupSize; ++i)
            {
                _groupResult[i] = &(g[i]);
            }
            return _groupResult;
        }

        Value const* getCurrentState() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return &(_values[_bucketIter->idx + _groupSize]);
        }

        vector<Value const*> const& getStateVector()
        {
            Value const* s = getCurrentState();
            for(size_t i =0; i<_numAggs; ++i)
            {
                _aggStateResult[i] = &(s[i]);
            }
            return _aggStateResult;
        }

    };

    const_iterator getIterator() const
    {
        return const_iterator(_values, _buckets, _hashes, _groupSize, _numAggs, _numHashBuckets);
    }

    void logStuff()
    {
        LOG4CXX_DEBUG(logger, "AHTSTAT hashes "<<_hashes.size()<<" groups "<<_numGroups<<" large_vals "<<_largeValueMemory<<" total "<<usedBytes());
    }
};

} } //namespace scidb::grouped_aggregate

#endif
