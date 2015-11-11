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
class ValueChain : private mgd::set<Value, AttributeComparator>
{
private:
    typedef mgd::set<Value, AttributeComparator> super;

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

//    ValueChain(super::allocator_type const& a, ValueChain const& c):
//    	super(a, c.begin(), c.end())
//    {}
//
//    ValueChain(mgd::set<Value, AttributeComparator>::allocator_type const& a, BOOST_RV_REF(ValueChain) c):
//        super(a, c.begin(), c.end())
//    {}

    bool insert(Value const& item)
    {
        return super::insert(item).second;
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

    HashBucket(ArenaPtr const& arena):
        super(arena.get())
    {}

    ~HashBucket()
    {}

    bool insert(ArenaPtr const& arena, AttributeComparator const& comparator, uint64_t hash, Value const& item)
    {
        ValueChain& v = super::insert(std::make_pair(hash, ValueChain(arena, comparator))).first->second;
        return v.insert(item);
    }
};

//XXX: do not inherit
class MemoryHashTable
{
private:
    mgd::vector < HashBucket >_data;
    ArenaPtr _arena;
    mgd::set<size_t> _occupiedBuckets;
    size_t _numValues;
    size_t _numUsedBuckets;
    AttributeComparator const _comparator;
    size_t _usedValueBytes;

public:
    static size_t const NUM_BUCKETS      = 1000037;

    MemoryHashTable(AttributeComparator const& comparator, ArenaPtr const& arena):
        _data(arena.get(), NUM_BUCKETS, HashBucket(arena)),
        _arena(arena),
        _occupiedBuckets(arena.get()),
        _numValues(0),
        _numUsedBuckets(0),
        _comparator(comparator),
        _usedValueBytes(0)
    {}

    bool insert(Value const& item)
    {
        uint64_t hash = hashValue(item);
        uint64_t bucketNo = hash % NUM_BUCKETS;
        HashBucket& bucket = _data[bucketNo];
        if ( bucket.empty())
        {
            _occupiedBuckets.insert(bucketNo);
            ++_numUsedBuckets;
        }
        if(bucket.insert(_arena, _comparator, hash, item))
        {
            ++_numValues;
            if(item.size() > 8)
            {
                _usedValueBytes += item.size();
            }
            return true;
        }
        return false;
    }

    bool empty() const
    {
        return _numValues == 0;
    }

    size_t numValues() const
    {
        return _numValues;
    }

    size_t usedBytes() const
    {
        return _arena->allocated() + _usedValueBytes;
    }

    class const_iterator
    {
    private:
        mgd::vector <HashBucket> const& _data;
        mgd::set<size_t> const& _occupiedBuckets;
        mgd::set<size_t>::const_iterator _tableIter;
        HashBucket::const_iterator _bucketIter;
        ValueChain::const_iterator _chainIter;

    public:
        const_iterator(mgd::vector <HashBucket> const& data,
                       mgd::set<size_t> const& occupiedBuckets):
          _data(data),
          _occupiedBuckets(occupiedBuckets)
        {
            restart();
        }

        void restart()
        {
            _tableIter = _occupiedBuckets.begin();
            if(_tableIter != _occupiedBuckets.end())
            {
                _bucketIter = _data[ *_tableIter ].begin();
                _chainIter = _bucketIter->second.begin();
            }
        }

        bool end() const
        {
            return _tableIter == _occupiedBuckets.end();
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
                ++(_bucketIter);
                if(_bucketIter == _data[ *_tableIter ].end() )
                {
                    ++(_tableIter);
                    if(end())
                    {
                        return;
                    }
                    _bucketIter = _data[ *_tableIter ].begin();
                }
                _chainIter = _bucketIter->second.begin();
            }
        }

        uint64_t getCurrentBucket() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return *_tableIter;
        }

        uint64_t getCurrentHash() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return _bucketIter->first;
        }

        Value const& getCurrentItem() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return (*_chainIter);
        }
    };

    const_iterator getIterator() const
    {
        return const_iterator(_data, _occupiedBuckets);
    }

    void dumpStatsToLog() const
    {
        LOG4CXX_DEBUG(logger, "Hashtable buckets " << _numUsedBuckets
                                     << " values " << _numValues
                                     << " bytes "  << usedBytes()
                                     << " arena " << *(_arena.get()) );
    }
};


} //namespace scidb
