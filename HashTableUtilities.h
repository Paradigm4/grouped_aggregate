#ifndef HASH_TABLE_UTILITIES
#define HASH_TABLE_UTILITIES

#include <query/Operator.h>
#include <query/AttributeComparator.h>
#include <util/arena/Set.h>
#include <util/arena/Map.h>
#include <util/arena/Vector.h>
#include <util/Arena.h>
#include <array/SortArray.h>
#include <array/TupleArray.h>

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

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.distinct"));

// MurmurHash2, 64-bit versions, by Austin Appleby
// From https://sites.google.com/site/murmurhash/
// MIT license
uint64_t mh64a ( const void * key, int len, unsigned int seed = 0x5C1DB123 )
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

static void EXCEPTION_ASSERT(bool cond)
{
    if (! cond)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
    }
}

//XXX: do not inherit
class ValueChain : private mgd::map<Value, Value, AttributeComparator>
{
private:
    typedef mgd::map<Value, Value, AttributeComparator> super;

    ValueChain()
    {}

public:
    using super::begin;
    using super::end;
    using super::empty;
    using super::const_iterator;

    ValueChain(ArenaPtr const& arena, AttributeComparator const& comparator):
        super(arena.get(), comparator)
    {}

    ~ValueChain()
    {}

    /**
     * @return the non-arena size change to the structure, after insertion.
     * The arena.allocated() can be used to track the size of all arena-backed memory,
     * for the remainder, add up the values returned by this method.
     * Assumes:
     * - Value default constructor creates size = 0
     * - Value with size <= 8 is stored in-body
     * - Value with size > 8 is allocated, without arena
     * All true as of 15.7
     */
    ssize_t insert(Value const& group, Value const& item, AggregatePtr& aggregate)
    {
        Value& state = super::operator [](group);
        ssize_t groupSizeDelta = 0;
        ssize_t initialStateSize = state.size();
        initialStateSize = initialStateSize > 8 ? initialStateSize : 0;
        if(state.getMissingReason() == 0) //first time
        {
            aggregate->initializeState(state);
            groupSizeDelta = group.size();
            groupSizeDelta = groupSizeDelta > 8 ? groupSizeDelta : 0;
        }
        aggregate->accumulateIfNeeded(state, item);
        ssize_t stateSize = state.size();
        stateSize = stateSize > 8 ? stateSize : 0;
        return groupSizeDelta + stateSize - initialStateSize;
    }

    bool contains(Value const& group) const
    {
        return super::count(group) != 0;
    }
};

//XXX: do not inherit
class HashBucket : private mgd::map<uint64_t, ValueChain >
{
private:
     typedef mgd::map<uint64_t, ValueChain > super;

    HashBucket()
    {}

public:
    using super::begin;
    using super::end;
    using super::empty;
    using super::const_iterator;
    using super::find;

    HashBucket(ArenaPtr const& arena):
        super(arena.get())
    {}

    ~HashBucket()
    {}

    ssize_t insert(ArenaPtr const& arena, AttributeComparator const& comparator, uint64_t hash, Value const& group, Value const& item, AggregatePtr& aggregate)
    {
        ValueChain& v = super::insert(std::make_pair(hash, ValueChain(arena, comparator))).first->second;
        return v.insert(group, item, aggregate);
    }

    bool contains(uint64_t hash, Value const& group) const
    {
        super::const_iterator iter = super::find(hash);
        if(iter==super::end())
        {
            return false;
        }
        ValueChain const& v = iter->second;
        return v.contains(group);
    }
};

//XXX: do not inherit
class AggregateHashTable
{
private:
    mgd::vector < HashBucket >_data;
    ArenaPtr _arena;
    mgd::vector <size_t> _hashes;
    size_t _numUsedBuckets;
    AttributeComparator const _comparator;
    ssize_t _usedValueBytes;

public:
    static size_t const NUM_BUCKETS      = 1000037;

    AggregateHashTable(AttributeComparator const& comparator, ArenaPtr const& arena):
        _data(arena.get(), NUM_BUCKETS, HashBucket(arena)),
        _arena(arena),
        _hashes(arena.get()),
        _numUsedBuckets(0),
        _comparator(comparator),
        _usedValueBytes(0)
    {}

    void insert(Value const& group, Value const& item, AggregatePtr& aggregate)
    {
        uint64_t hash = hashValue(group);
        uint64_t bucketNo = hash % NUM_BUCKETS;
        HashBucket& bucket = _data[bucketNo];
        if ( bucket.empty())
        {
            _hashes.push_back(hash);
            ++_numUsedBuckets;
        }
       _usedValueBytes += bucket.insert(_arena, _comparator, hash, group, item, aggregate);
    }

    bool contains(Value const& group, uint64_t& hash) const
    {
        hash = hashValue(group);
        uint64_t bucketNo = hash % NUM_BUCKETS;
        return _data[bucketNo].contains(hash, group);
    }

    size_t usedBytes() const
    {
        return _arena->allocated() + ( _usedValueBytes > 0 ? _usedValueBytes : 0);
    }

    class const_iterator
    {
    private:
        mgd::vector <HashBucket> const& _data;
        mgd::vector<size_t> const& _hashes;
        mgd::vector<size_t>::const_iterator _hashIter;
        HashBucket::const_iterator _bucketIter;
        ValueChain::const_iterator _chainIter;

    public:
        const_iterator(mgd::vector <HashBucket> const& data,
                       mgd::vector<size_t> const& hashes):
          _data(data),
          _hashes(hashes)
        {
            restart();
        }

        void restart()
        {
            _hashIter = _hashes.begin();
            if(_hashIter != _hashes.end())
            {
                size_t const hash = (*_hashIter);
                HashBucket const& b = _data[hash % AggregateHashTable::NUM_BUCKETS];
                _bucketIter = b.find(hash);
                _chainIter = _bucketIter->second.begin();
            }
        }

        bool end() const
        {
            return _hashIter == _hashes.end();
        }

        void next()
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "iterating past end";
            }
            ++(_chainIter);
            if (_chainIter == _bucketIter->second.end())
            {
                ++(_hashIter);
                if(end())
                {
                    return;
                }
                size_t const hash = (*_hashIter);
                HashBucket const& b = _data[hash % AggregateHashTable::NUM_BUCKETS];
                _bucketIter = b.find(hash);
                _chainIter = _bucketIter->second.begin();
            }
        }

        uint64_t getCurrentHash() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return (*_hashIter);
        }

        Value const& getCurrentGroup() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return _chainIter->first;
        }

        Value const& getCurrentState() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return _chainIter->second;
        }
    };

    void sortKeys()
    {
        std::sort(_hashes.begin(), _hashes.end());
    }

    const_iterator getIterator() const
    {
        return const_iterator(_data, _hashes);
    }

    void dumpStatsToLog() const
    {
        LOG4CXX_DEBUG(logger, "Hashtable buckets " << _numUsedBuckets
                                     << " bytes "  << usedBytes()
                                     << " arena " << *(_arena.get()) );
    }
};

} } //namespace scidb::grouped_aggregate

#endif
