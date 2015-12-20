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


static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.aht"));

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

struct Triplet
{
    uint64_t hash;
    Value    group;
    Value    state;

    Triplet(uint64_t const h, Value const& g):
      hash(h), group(g)
    {}
};

class AggregateHashTable
{
private:
    ArenaPtr _arena;
    mgd::vector< mgd::list<Triplet> > _data;
    mgd::list<Triplet>::iterator _iter;
    AttributeComparator     _comparator;
    mgd::vector<uint64_t> _hashes;
    Value    *_lastGroup;
    Value    *_lastState;
    ssize_t   _stateSizeOverflow;
    size_t    _numGroups;

public:
    static size_t const NUM_BUCKETS      = 1000037;

    AggregateHashTable(AttributeComparator const& comparator, ArenaPtr const& arena):
        _arena(arena),
        _data(_arena, NUM_BUCKETS, mgd::list<Triplet>(_arena)),
        _comparator(comparator),
        _hashes(_arena, 0),
        _lastGroup(0),
        _lastState(0),
        _stateSizeOverflow(0),
        _numGroups(0)
    {}

    void insert(Value const& group, Value const& item, AggregatePtr& aggregate)
    {
        if(_lastGroup!=0 && group == (*_lastGroup))
        {
            aggregate->accumulateIfNeeded(*_lastState, item);
            return;
        }
        uint64_t hash = hashValue(group);
        mgd::list<Triplet>& list = _data[hash % NUM_BUCKETS];
        _iter = list.begin();
        bool seenHash = false;
        while(_iter != list.end() &&  (_iter->hash < hash || (_iter->hash == hash && _comparator(_iter->group, group)) ) )
        {
            ++_iter;
            if(_iter->hash == hash)
            {
                seenHash = true;
            }
        }
        ssize_t initialStateSize = 0;
        if(_iter != list.end() && _iter->hash == hash && _iter->group == group)
        {
            initialStateSize = _iter->state.size() > 8 ? _iter->state.size() : 0;
        }
        else
        {
            if(!seenHash)
            {
                _hashes.push_back(hash);
            }
            _iter = list.emplace(_iter, hash, group);
            ++_numGroups;
            aggregate->initializeState( _iter->state);
        }
        aggregate->accumulateIfNeeded(_iter->state, item);
        ssize_t finalStateSize   = _iter->state.size() > 8 ? _iter->state.size() : 0;
        _stateSizeOverflow += (finalStateSize - initialStateSize);
        _lastGroup = &_iter->group;
        _lastState = &_iter->state;
    }

    /**
     * @param[out] hash computes the hash of group as a side-effect
     * @return true if the table contains the group, false otherwise
     */
    bool contains(Value const& group, uint64_t& hash) const
    {
        hash = hashValue(group);
        mgd::list<Triplet> const& list = _data[hash % NUM_BUCKETS];
        mgd::list<Triplet>::const_iterator _citer = list.begin();
        while(_citer != list.end() &&  (_citer->hash < hash || (_citer->hash == hash && _comparator(_citer->group, group)) ) )
        {
            ++_citer;
        }
        if(_citer != list.end() && _citer->hash == hash && _citer->group == group)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * @return the total amount of bytes used by the structure
     */
    size_t usedBytes() const
    {
        if(_stateSizeOverflow < 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)<<" inconsistent state size overflow";
        }
        return _arena->allocated() + _stateSizeOverflow;
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

    void logStuff()
    {
        LOG4CXX_DEBUG(logger, "AHTSTAT HASHES "<<_hashes.size()<<" GROUPS "<<_numGroups<<" ALLOC "<<_arena->allocated()<<" OVER "<<_stateSizeOverflow<<" BYTES "<<usedBytes());
    }

    class const_iterator
    {
    private:
        mgd::vector <mgd::list<Triplet> > const& _data;
        mgd::vector<size_t> const& _hashes;
        mgd::vector<size_t>::const_iterator _hashIter;
        uint64_t _currHash;
        mgd::list<Triplet>::const_iterator _listIter;

    public:
        /**
         * To get one, call AggregateHashTable::getIterator
         */
        const_iterator(mgd::vector <mgd::list<Triplet> > const& data,
                       mgd::vector<size_t> const& hashes):
          _data(data),
          _hashes(hashes)
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
                mgd::list<Triplet> const& l = _data[_currHash % AggregateHashTable::NUM_BUCKETS];
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
                mgd::list<Triplet> const& l = _data[_currHash % AggregateHashTable::NUM_BUCKETS];
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

        Value const& getCurrentGroup() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return _listIter->group;
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
        return const_iterator(_data, _hashes);
    }
};

} } //namespace scidb::grouped_aggregate

#endif
