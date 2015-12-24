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

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.aht"));

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

uint64_t hashValue( Value const& val)
{
    if (val.isNull() || val.size() == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "hashValue call";
    }
    return mh64a(val.data(), val.size());
}

uint64_t hashGroup(std::vector<Value const*> const& group, size_t const groupSize)
{
    if(groupSize == 1)
    {
        Value const* v = group[0];
        return hashValue(*v);
    }
    size_t totalSize = 0;
    for(size_t i =0; i<groupSize; ++i)
    {
        totalSize += group[i]->size();
    }
    std::vector<char> buf (totalSize);  //TODO: get rid of this allocation
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
    mgd::vector<Value> group;
    Value    state;

    HashTableEntry(ArenaPtr& arena, size_t const groupSize, uint64_t const h, std::vector<Value const*> g):
      hash(h), group(arena, groupSize)
    {
        for(size_t i=0; i<groupSize; ++i)
        {
            group[i] = *(g[i]);
        }
    }

    Value const* groupPtr() const
    {
        return &(group[0]);
    }
};

class AggregateHashTable
{
private:
    ArenaPtr                                 _arena;
    size_t const                             _groupSize;
    mgd::vector< mgd::list<HashTableEntry> > _data;
    mgd::list<HashTableEntry>::iterator      _iter;
    Settings const&                          _settings;
    mgd::vector<uint64_t>                    _hashes;
    Value const*                             _lastGroup;
    Value    *                               _lastState;
    ssize_t                                  _largeValueMemory;
    size_t                                   _numGroups;

public:
    static size_t const NUM_BUCKETS      = 1000037;

    AggregateHashTable(Settings const& settings, ArenaPtr const& arena):
        _arena(arena),
        _groupSize(settings.getGroupSize()),
        _data(_arena, NUM_BUCKETS, mgd::list<HashTableEntry>(_arena)),
        _settings(settings),
        _hashes(_arena, 0),
        _lastGroup(NULL),
        _lastState(0),
        _largeValueMemory(0),
        _numGroups(0)
    {}

    void insert(std::vector<Value const*> const& group, Value const& item, AggregatePtr& aggregate)
    {
        if(_lastGroup != NULL && _settings.groupEqual(_lastGroup, group))
        {
            aggregate->accumulateIfNeeded(*_lastState, item);
            return;
        }
        uint64_t hash = hashGroup(group, _groupSize);
        mgd::list<HashTableEntry>& list = _data[hash % NUM_BUCKETS];
        _iter = list.begin();
        bool seenHash = false;
        while(_iter != list.end() &&  (_iter->hash < hash || (_iter->hash == hash && _settings.groupLess(_iter->groupPtr(), group))))
        {
            if(_iter->hash == hash)
            {
                seenHash = true;
            }
            ++_iter;
        }
        if(_iter != list.end() && _iter->hash == hash) //in case the first element in the table matches and is bigger group
        {
            seenHash = true;
        }
        ssize_t initialStateSize = 0;
        if(_iter != list.end() && _iter->hash == hash && _settings.groupEqual(_iter->groupPtr(), group))
        {
            initialStateSize = _iter->state.isLarge() ? _iter->state.size() : 0;
        }
        else
        {
            if(!seenHash)
            {
                _hashes.push_back(hash);
            }
            _iter = list.emplace(_iter, _arena, _groupSize, hash, group);
            for(size_t i =0; i<_groupSize; ++i)
            {
                if(group[i]->isLarge())
                {
                    _largeValueMemory += group[i]->size();
                }
            }
            ++_numGroups;
            aggregate->initializeState( _iter->state);
        }
        aggregate->accumulateIfNeeded(_iter->state, item);
        ssize_t finalStateSize   = _iter->state.isLarge() ? _iter->state.size() : 0;
        _largeValueMemory += (finalStateSize - initialStateSize);
        _lastGroup = _iter->groupPtr();
        _lastState = &_iter->state;
    }

    /**
     * @param[out] hash computes the hash of group as a side-effect
     * @return true if the table contains the group, false otherwise
     */
    bool contains(std::vector<Value const*> const& group, uint64_t& hash) const
    {
        hash = hashGroup(group, _groupSize);
        mgd::list<HashTableEntry> const& list = _data[hash % NUM_BUCKETS];
        mgd::list<HashTableEntry>::const_iterator citer = list.begin();
        while(citer != list.end())
        {
            if(citer->hash == hash && _settings.groupEqual(citer->groupPtr(), group))
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
        mgd::vector <mgd::list<HashTableEntry> > const& _data;
        mgd::vector<size_t> const& _hashes;
        size_t const _groupSize;
        mgd::vector<size_t>::const_iterator _hashIter;
        uint64_t _currHash;
        mgd::list<HashTableEntry>::const_iterator _listIter;
        vector<Value const*> _groupResult;

    public:
        /**
         * To get one, call AggregateHashTable::getIterator
         */
        const_iterator(mgd::vector <mgd::list<HashTableEntry> > const& data,
                       mgd::vector<size_t> const& hashes,
                       size_t const groupSize):
          _data(data),
          _hashes(hashes),
          _groupSize(groupSize),
          _groupResult(groupSize, NULL)
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
                mgd::list<HashTableEntry> const& l = _data[_currHash % AggregateHashTable::NUM_BUCKETS];
                _listIter = l.begin();
                while(_listIter->hash != _currHash)
                {
                    ++_listIter;
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
            ++(_listIter);
            if (_listIter->hash != _currHash)
            {
                ++(_hashIter);
                if(end())
                {
                    return;
                }
                _currHash = (*_hashIter);
                mgd::list<HashTableEntry> const& l = _data[_currHash % AggregateHashTable::NUM_BUCKETS];
                _listIter = l.begin();
                while(_listIter->hash != _currHash)
                {
                   ++_listIter;
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
            return _listIter->groupPtr();
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

        Value const& getCurrentState() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return _listIter->state;
        }
    };

    const_iterator getIterator() const
    {
        return const_iterator(_data, _hashes, _groupSize);
    }

    void logStuff()
    {
        LOG4CXX_DEBUG(logger, "AHTSTAT hashes "<<_hashes.size()<<" groups "<<_numGroups<<" alloc "<<_arena->allocated()<<" large_vals "<<_largeValueMemory<<" total "<<usedBytes());
    }
};

} } //namespace scidb::grouped_aggregate

#endif
