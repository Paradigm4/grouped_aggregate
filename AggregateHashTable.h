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

#include <query/PhysicalOperator.h>
#include <query/AttributeComparator.h>
#include <util/arena/Map.h>
#include <util/arena/Vector.h>
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

/**
 * We attempt to hash groups of 1 or more SciDB values (group) + 1 or more SciDB values (states) into a in-memory structure
 * keyed by the group that allows quick addition or lookup.
 *
 * This structure is made specifically for aggregation. Semantically it is a lot like a table where some columns are "groups" and
 * other columns are "states" for example:
 *
 * group0, group1, state0
 * "alex",      1,    5.4
 *  "bob",      2,    8.2
 * "kyle",      1,    2.4
 *
 * Data are added to the structure one tuple (group + state) at a time. Adding a group that already exists results in
 * aggregate accumulation of the states. For example inserting this tuple:
 * "alex",      1,    1.5
 *
 * results in:
 *
 * group0, group1, state0
 * "alex",      1,    6.9   <-- assuming the aggregate is a floating point sum
 *  "bob",      2,    8.2
 * "kyle",      1,    2.4
 *
 * Note there need not be only one state per group and the states may be variable-sized, depending on which aggregate(s) are used
 *
 * This structure supports overall grouped aggregation of SciDB and uses hashing internally. The hashes computed by this table ar
 * used for other key purposes:
 *  - distribute data by hash for good smearing across the cluster
 *  - order data by hash, then group for fast merge of results that come from different instances.
 *
 * Moreover, a memory limit is imposed over the table and the total memory footprint is accounted for carefully.
 *
 * More moreover, we don't know how many groups we will have until we insert all the data. The input may be terabytes in size, bu
 * easily condense to 10 groups that fit neatly in memory.
 *
 * For those reasons, some key properties are implemented
 * 1) Hash values are try to be uniform smeared 32-bit between 0 and 2^32 - for even subsequent data distribution in the cluster.
 * 2) Hhash values are accessible outside the structure. This is one reason why we didn't use an STL structure.
 * 3) Iteration over the data is supported in order of increasing hash. This is another reason why we didn't use an STL structure
 * 4) No rehashing. We know the memory limit, so we start out with a large enough number of buckets.
 * Didn't want to be in the business of copying over data or finidng nearest primes.
 */

class AggregateHashTable
{
private:
    //-----------------------------------------------------------------------------
    // MurmurHash3 was written by Austin Appleby, and is placed in the public
    // domain. The author hereby disclaims copyright to this source code.
    #define ROT32(x, y) ((x << y) | (x >> (32 - y))) // avoid effort
    static uint32_t murmur3_32(const char *key, uint32_t len, uint32_t const seed = 0x5C1DB123)
    {
        static const uint32_t c1 = 0xcc9e2d51;
        static const uint32_t c2 = 0x1b873593;
        static const uint32_t r1 = 15;
        static const uint32_t r2 = 13;
        static const uint32_t m = 5;
        static const uint32_t n = 0xe6546b64;
        uint32_t hash = seed;
        const int nblocks = len / 4;
        const uint32_t *blocks = (const uint32_t *) key;
        int i;
        uint32_t k;
        for (i = 0; i < nblocks; i++)
        {
            k = blocks[i];
            k *= c1;
            k = ROT32(k, r1);
            k *= c2;
            hash ^= k;
            hash = ROT32(hash, r2) * m + n;
        }
        const uint8_t *tail = (const uint8_t *) (key + nblocks * 4);
        uint32_t k1 = 0;
        switch (len & 3)
        {
        case 3:
            k1 ^= tail[2] << 16;
        case 2:
            k1 ^= tail[1] << 8;
        case 1:
            k1 ^= tail[0];
            k1 *= c1;
            k1 = ROT32(k1, r1);
            k1 *= c2;
            hash ^= k1;
        }
        hash ^= len;
        hash ^= (hash >> 16);
        hash *= 0x85ebca6b;
        hash ^= (hash >> 13);
        hash *= 0xc2b2ae35;
        hash ^= (hash >> 16);
        return hash;
    }
    //End of MurmurHash3 Implementation
    //-----------------------------------------------------------------------------

    uint32_t hashGroup(std::vector<Value const*> const& group, size_t const groupSize) const
    {
        uint32_t totalSize = 0;
        for(size_t i =0; i<groupSize; ++i)
        {
            totalSize += group[i]->size();
        }
        if(_hashBuf.size() < totalSize)
        {
            _hashBuf.resize(totalSize);
        }
        char* ch = &_hashBuf[0];
        for(size_t i =0; i<groupSize; ++i)
        {
            memcpy(ch, group[i]->data(), group[i]->size());
            ch += group[i]->size();
        }
        return murmur3_32(&_hashBuf[0], totalSize);
    }

    struct HashTableEntry
    {
        size_t idx;
        HashTableEntry* next;
        HashTableEntry(size_t const idx, HashTableEntry* next):
            idx(idx), next(next)
        {}
    };

    Settings&                                _settings;
    ArenaPtr                                 _arena;
    size_t const                             _groupSize;
    size_t const                             _numAggs;
    uint32_t const                           _numHashBuckets;
    mgd::vector<HashTableEntry*>             _buckets;
    mgd::vector<Value>                       _values;
    Value const*                             _lastGroup;
    Value *                                  _lastState;
    ssize_t                                  _largeValueMemory;
    size_t                                   _numHashes;
    size_t                                   _numGroups;
    mutable vector<char>                     _hashBuf;

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
        _numHashBuckets(safe_static_cast<uint32_t>(settings.getNumHashBuckets())),
        _buckets(_arena, _numHashBuckets, NULL),
        _values(_arena, 0),
        _lastGroup(NULL),
        _lastState(NULL),
        _largeValueMemory(0),
        _numHashes(0),
        _numGroups(0),
        _hashBuf(64)
    {}

    void insert(std::vector<Value const*> const& group, std::vector<Value const*> const& input)
    {
        if(_lastGroup != NULL && _settings.groupEqual(_lastGroup, group))
        {
            accumulateStates(_lastState, input);
            return;
        }

        uint32_t hash = hashGroup(group, _groupSize) % _numHashBuckets;
        bool newGroup = true;
        bool newHash = true;
        HashTableEntry** entry = &(_buckets[hash]);
        while( (*entry) != NULL)
        {
            newHash = false;
            HashTableEntry** next = &((*entry)->next);
            Value const* storedGrp = getGroup( (*entry)->idx);
            if(_settings.groupEqual(storedGrp, group))
            {
                newGroup = false;
                break;
            }
            else if (! _settings.groupLess(storedGrp, group))
            {
                break;
            }
            entry = next;
        }
        if(newGroup)
        {
            if(newHash)
            {
                ++_numHashes;
            }
            ++_numGroups;
            size_t idx = addNewGroup(group, input);
            HashTableEntry* newEntry = ((HashTableEntry*)_arena->allocate(sizeof(HashTableEntry)));
            *newEntry = HashTableEntry(idx,*entry);
            *entry = newEntry;
            _lastGroup = getGroup(idx);
            _lastState = getStates(idx);
        }
        else
        {
            Value const* storedGrp = getGroup( (*entry)->idx);
            Value* storedStates = getStates((*entry)->idx);
            accumulateStates(storedStates, input);
            _lastGroup = storedGrp;
            _lastState = storedStates;
        }
    }

    /**
     * @param[out] hash computes the hash of group as a side-effect
     * @return true if the table contains the group, false otherwise
     */
    bool contains(std::vector<Value const*> const& group, uint32_t& hash) const
    {
        hash = hashGroup(group, _groupSize) % _numHashBuckets;
        HashTableEntry const* bucket = _buckets[hash];
        while(bucket != NULL)
        {
            if(_settings.groupEqual(getGroup(bucket->idx), group))
            {
                return true;
            }
            else if(! _settings.groupLess(getGroup(bucket->idx), group))
            {
                return false;
            }
            bucket = bucket->next;
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
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)<<"inconsistent state size overflow";
        }
        return _arena->allocated() + _largeValueMemory;
    }

    class const_iterator
    {
    private:
        AggregateHashTable const* _table;
        uint32_t _currHash;
        HashTableEntry const* _bucket;
        vector<Value const*> _groupResult;
        vector<Value const*> _aggStateResult;

    public:
        /**
         * To get one, call AggregateHashTable::getIterator
         */
        const_iterator(AggregateHashTable const* table):
          _table(table),
          _groupResult(_table->_groupSize, NULL),
          _aggStateResult(_table->_numAggs, NULL)
        {
            restart();
        }

        /**
         * Set the iterator at the first hash in the table
         */
        void restart()
        {
            _currHash = 0;
            do
            {
                _bucket = _table->_buckets[_currHash];
                if(_bucket != NULL)
                {
                    break;
                }
                ++_currHash;
            } while(_currHash < _table->_numHashBuckets);
        }

        /**
         * @return true if the iterator has no more items, false otherwise
         */
        bool end() const
        {
            return _currHash >= _table->_numHashBuckets;
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
            _bucket = _bucket->next;
            while ( _bucket == NULL )
            {
                ++(_currHash);
                if(end())
                {
                    return;
                }
                _bucket = _table->_buckets[_currHash];
            }
        }

        //GETTERS
        uint32_t getCurrentHash() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return _currHash;
        }

        Value const* getCurrentGroup() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return &(_table->_values[_bucket->idx]);
        }

        vector<Value const*> const& getGroupVector()
        {
            Value const* g = getCurrentGroup();
            for(size_t i =0; i<_table->_groupSize; ++i)
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
            return &(_table->_values[_bucket->idx + _table->_groupSize]);
        }

        vector<Value const*> const& getStateVector()
        {
            Value const* s = getCurrentState();
            for(size_t i =0; i<_table->_numAggs; ++i)
            {
                _aggStateResult[i] = &(s[i]);
            }
            return _aggStateResult;
        }

    };

    const_iterator getIterator() const
    {
        return const_iterator(this);
    }

    void logStuff()
    {
        LOG4CXX_DEBUG(logger, "GAGG hashes "<<_numHashes<<" groups "<<_numGroups<<" large_vals "<<_largeValueMemory<<" total "<<usedBytes());
    }
};

} } //namespace scidb::grouped_aggregate

#endif
