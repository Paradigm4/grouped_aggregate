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
 *
 * At the moment, the structure has a maximum size limit of around ~4.3 Billion groups (on a single instance) because it uses
 * a 4-byte integer index. That should be easy to change to 8 bytes if needed.
 */


/**
 * We'd like to see a load factor of 4 or less. A group occupies at least 32 bytes in the structure,
 * usually more - depending on how many values and states there are and also whether they are variable sized.
 * An empty bucket is an 8-byte pointer. So the ratio of group data / bucket overhead is at least 16.
 * We that in mind we just pick a few primes for the most commonly used memory limits. We start with that
 * many buckets and don't bother rehashing:
 *
 * memory_limit_MB     max_groups    desired_buckets   nearest_prime   buckets_overhead_MB
 *             128        4194304            1048576         1048573                     8
 *             256        8388608            2097152         2097143                    16
 *             512       16777216            4194304         4194301                    32
 *           1,024       33554432            8388608         8388617                    64
 *           2,048       67108864           16777216        16777213                   128
 *           4,096      134217728           33554432        33554467                   256
 *           8,192      268435456           67108864        67108859                   512
 *          16,384      536870912          134217728       134217757                 1,024
 *          32,768     1073741824          268435456       268435459                 2,048
 *          65,536     2147483648          536870912       536870909                 4,096
 *         131,072     4294967296         1073741824      1073741827                 8,192
 *            more                                        2147483647                16,384
 */

static const size_t NUM_SIZES = 12;
static const size_t memLimits[NUM_SIZES]  = {    128,     256,     512,    1024,     2048,     4096,     8192,     16384,     32768,     65536,     131072,  ((size_t)-1) };
static const size_t tableSizes[NUM_SIZES] = {1048573, 2097143, 4194301, 8388617, 16777213, 33554467, 67108859, 134217757, 268435459, 536870909, 1073741827,    2147483647 };

static size_t pickTableSize(size_t memLimit)
{
   for(size_t i =0; i<NUM_SIZES; ++i)
   {
       if(memLimit <= memLimits[i])
       {
           return tableSizes[i];
       }
   }
   return tableSizes[NUM_SIZES-1];
}

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

    static uint32_t hashGroup(std::vector<Value const*> const& group, size_t const groupSize)
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
        return murmur3_32(&buf[0], totalSize);
    }

    struct HashTableEntry
    {
        uint32_t hash;
        uint32_t idx;
        HashTableEntry* next;

        HashTableEntry(uint32_t const hash, uint32_t const idx, HashTableEntry* next):
            hash(hash), idx(idx), next(next)
        {}
    };

    Settings&                                _settings;
    ArenaPtr                                 _arena;
    size_t const                             _groupSize;
    size_t const                             _numAggs;
    size_t const                             _numHashBuckets;
    mgd::vector<uint32_t>                    _hashes;
    mgd::vector<HashTableEntry*>             _buckets;
    mgd::vector<Value>                       _values;
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

    uint32_t addNewGroup(std::vector<Value const*> const& group, std::vector<Value const*> const& input)
    {
        if(_values.size() + _numAggs + _groupSize > std::numeric_limits<uint32_t>::max())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)<<"hash table size limit exceeded";
        }
        uint32_t idx = _values.size();
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

    Value const* getGroup(uint32_t const idx) const
    {
        return &(_values[idx]);
    }

    Value* getStates(uint32_t const idx)
    {
        return &(_values[idx + _groupSize]);
    }

public:
    AggregateHashTable(Settings& settings, ArenaPtr const& arena):
        _settings(settings),
        _arena(arena),
        _groupSize(settings.getGroupSize()),
        _numAggs(settings.getNumAggs()),
        _numHashBuckets( settings.numHashBucketsSet() ? settings.getNumHashBuckets() : pickTableSize(settings.getMaxTableSize() / (1024*1024) )),
        _hashes(_arena, 0),
        _buckets(_arena, _numHashBuckets, NULL),
        _values(_arena, 0),
        _lastGroup(NULL),
        _lastState(NULL),
        _largeValueMemory(0),
        _numGroups(0)
    {
        LOG4CXX_DEBUG(logger, "GAGG buckets "<<_numHashBuckets);

    }

    void insert(std::vector<Value const*> const& group, std::vector<Value const*> const& input)
    {
        if(_lastGroup != NULL && _settings.groupEqual(_lastGroup, group))
        {
            accumulateStates(_lastState, input);
            return;
        }
        uint32_t hash = hashGroup(group, _groupSize);
        bool newHash = true;
        bool newGroup = true;
        HashTableEntry** entry = &(_buckets[hash % _numHashBuckets]);
        while( (*entry) != NULL)
        {
            HashTableEntry** next = &((*entry)->next);
            if((*entry)->hash > hash)
            {
                break;
            }
            else if((*entry) -> hash == hash)
            {
                newHash = false;
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
            }
            entry = next;
        }
        if(newGroup)
        {
            if(newHash)
            {
                _hashes.push_back(hash);
            }
            ++_numGroups;
            uint32_t idx = addNewGroup(group, input);
            HashTableEntry* newEntry = ((HashTableEntry*)_arena->allocate(sizeof(HashTableEntry)));
            *newEntry = HashTableEntry(hash,idx,*entry);
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
        hash = hashGroup(group, _groupSize);
        HashTableEntry const* bucket = _buckets[hash % _numHashBuckets];
        while(bucket != NULL)
        {
            if(bucket->hash == hash && _settings.groupEqual(getGroup(bucket->idx), group))
            {
                return true;
            }
            if(bucket->hash > hash)
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

    /**
     * Sort the hash keys in the table. A subsequent call to getIterator shall return an iterator
     * that will iterate over the data in the order of increasing hash, then increasing group.
     * If this is not called, the iterator will return over the hashes in arbitrary order.
     * Yeah algorithmically it's all n lg n but in practice this is hella faster than sorting
     * SciDB values.
     */
    void sortKeys()
    {
        //TODO: make this a radix thing.
        std::sort(_hashes.begin(), _hashes.end());
    }

    class const_iterator
    {
    private:
        mgd::vector <Value> const& _values;
        mgd::vector <HashTableEntry* > const& _buckets;
        mgd::vector<uint32_t> const& _hashes;
        size_t const _groupSize;
        size_t const _numAggs;
        size_t const _numHashBuckets;
        mgd::vector<uint32_t>::const_iterator _hashIter;
        uint32_t _currHash;
        HashTableEntry const* _bucket;
        vector<Value const*> _groupResult;
        vector<Value const*> _aggStateResult;

    public:
        /**
         * To get one, call AggregateHashTable::getIterator
         */
        const_iterator(mgd::vector<Value> const& values,
                       mgd::vector <HashTableEntry*> const& buckets,
                       mgd::vector<uint32_t> const& hashes,
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
                _bucket = (_buckets[_currHash % _numHashBuckets]);
                while(_bucket->hash != _currHash)
                {
                    _bucket = _bucket->next;
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
            _bucket = _bucket->next;
            if ( _bucket == NULL || _bucket->hash != _currHash)
            {
                ++(_hashIter);
                if(end())
                {
                    return;
                }
                _currHash = (*_hashIter);
                _bucket = _buckets[_currHash % _numHashBuckets];
                while(_bucket->hash != _currHash)
                {
                   _bucket = _bucket->next;
                }
            }
        }

        //GETTERS
        uint32_t getCurrentHash() const
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
            return &(_values[_bucket->idx]);
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
            return &(_values[_bucket->idx + _groupSize]);
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
        LOG4CXX_DEBUG(logger, "GAGG hashes "<<_hashes.size()<<" groups "<<_numGroups<<" large_vals "<<_largeValueMemory<<" total "<<usedBytes());
    }
};

} } //namespace scidb::grouped_aggregate

#endif
