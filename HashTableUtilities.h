#include <query/Operator.h>
#include <query/AttributeComparator.h>
#include <util/arena/Set.h>
#include <util/arena/Map.h>
#include <util/arena/Vector.h>
#include <util/Arena.h>
#include <array/SortArray.h>
#include <array/TupleArray.h>
//#include <system/Exceptions.h>

namespace scidb
{

using scidb::arena::Options;
using scidb::arena::ArenaPtr;
using scidb::arena::newArena;
using scidb::SortArray;
using std::shared_ptr;
using std::dynamic_pointer_cast;

//class SortArray;
//class TupleComparator;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.distinct"));

static int64_t const ARRAY_CHUNK_SIZE = 1000000;

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

class MemArrayAppender
{
private:
    shared_ptr<Query> const&           _query;
    size_t const                       _nAttrs;
    size_t const                       _chunkSize;
    size_t const                       _startingChunk;
    size_t const                       _skipInterval;
    Coordinates                        _cellPos;
    Coordinates                        _chunkPos;
    vector<shared_ptr<ArrayIterator> > _aiters;
    vector<shared_ptr<ChunkIterator> > _citers;
    Value _buf;

    void advance()
    {
        ++_cellPos[0];
        if (_cellPos[0] % _chunkSize == 0)
        {
            if (_citers[0])
            {
                for(size_t i=0; i<_nAttrs; ++i)
                {
                    _citers[i]->flush();
                }
            }
            _chunkPos[0] += _skipInterval;
            _citers[0]=_aiters[0] ->newChunk(_chunkPos).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE);
            for(size_t i=1; i<_nAttrs; ++i)
            {
                _citers[i] =_aiters[i] ->newChunk(_chunkPos).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE |
                                                                                 ChunkIterator::NO_EMPTY_CHECK);
            }
            _cellPos[0] = _chunkPos[0];
        }
        for(size_t i=0; i<_nAttrs; ++i)
        {
            _citers[i] ->setPosition(_cellPos);
        }
    }

public:
    MemArrayAppender(shared_ptr<MemArray> memArray,
                     shared_ptr<Query> const& query,
                     size_t startingChunk = 0,
                     size_t skipInterval  = 1000000):
       _query(query),
       _nAttrs(memArray->getArrayDesc().getAttributes(true).size()),
       _chunkSize(memArray->getArrayDesc().getDimensions()[0].getChunkInterval()),
       _startingChunk(startingChunk),
       _skipInterval(skipInterval),
       _cellPos(1,startingChunk - 1),
       _chunkPos(1,startingChunk - skipInterval),
       _aiters(_nAttrs),
       _citers(_nAttrs)
    {

    	EXCEPTION_ASSERT(memArray->getArrayDesc().getDimensions().size() == 1 &&
                         memArray->getArrayDesc().getDimensions()[0].getStartMin() == 0 &&
                         skipInterval % memArray->getArrayDesc().getDimensions()[0].getChunkInterval() == 0);
        for (AttributeID i = 0; i<_nAttrs; ++i)
        {
            _aiters[i] = memArray->getIterator(i);
        }
    }

    void addValues(Value const& v0, Value const& v1, Value const& v2)
    {
        EXCEPTION_ASSERT(_nAttrs==3);
        advance();
        _citers[0]->writeItem(v0);
        _citers[1]->writeItem(v1);
        _citers[2]->writeItem(v2);
    }

    void addValues(Value const& v0,uint64_t const& v1)
     {
         EXCEPTION_ASSERT(_nAttrs==2);
         advance();

         _citers[0]->writeItem(v0);
         _buf.setUint64(v1);
         _citers[1]->writeItem(_buf);

     }

    void addValues(Value const& v0, uint64_t const v1, uint64_t const v2)
    {
        EXCEPTION_ASSERT(_nAttrs==3);
        advance();
        _citers[0]->writeItem(v0);
        _buf.setUint64(v1);
        _citers[1]->writeItem(_buf);
        _buf.setUint64(v2);
        _citers[2]->writeItem(_buf);
    }

    void addValues(Value const& v0)
    {
        EXCEPTION_ASSERT(_nAttrs==1);
        advance();
        _citers[0]->writeItem(v0);
    }

    void release()
    {
        if (_citers[0])
        {
            for(size_t i=0; i<_nAttrs; ++i)
            {
                _citers[i]->flush();
                _citers[i].reset();
            }
        }
        for(size_t i=0; i<_nAttrs; ++i)
        {
            _aiters[i].reset();
        }
    }
};

//
//class MergeMemArrayAppender
//{
//private:
//    shared_ptr<Query> const&           _query;
//    size_t const                       _nAttrs;
//    size_t const                       _chunkSize;
//    size_t const                       _startingChunk;
//    size_t const                       _skipInterval;
//    Coordinates                        _cellPos;
//    Coordinates                        _chunkPos;
//    Coordinate                         _chunkNumber;
//    vector<shared_ptr<ArrayIterator> > _aiters;
//    vector<shared_ptr<ChunkIterator> > _citers;
//    Value _buf;
//    Coordinate                         _lastBucketIdVal;
//
//    void advance(Coordinate bucketId, Coordinate senderInstance)
//    {
//
//          _cellPos[0] = bucketId;
//          _cellPos[1] = senderInstance;
//          _cellPos[2] = _chunkNumber;
//        ++_cellPos[3];
//
//	    //dims.push_back(DimensionDesc("value_id", 0, MAX_COORDINATE, ARRAY_CHUNK_SIZE, 0));
//        //dims.push_back(DimensionDesc("bucket_id", 0, MAX_COORDINATE, 1, 0));
//        //dims.push_back(DimensionDesc("sender_instance", 0, MAX_COORDINATE, 1, 0));
//        //dims.push_back(DimensionDesc("chunk_number", 0, MAX_COORDINATE, 1, 0));
//        //LOG4CXX_DEBUG(logger, "Cell0:" << _cellPos[0] << " ,Cell1: " <<_cellPos[1] <<",_lastval:" << (_lastVal ) <<"bucket:" << bucketId);
//
//        //TODO: This needs to be cleaned up and commented. It's very hard to understand.
//        if ((_cellPos[3] % _chunkSize == 0) || (_cellPos[0] != _lastBucketIdVal) )
//        {
//
//        	if (_citers[0])
//            {
//                for(size_t i=0; i<_nAttrs; ++i)
//                {
//                    _citers[i]->flush();
//                }
//            }
//
//        	if (_cellPos[3] % _chunkSize == 0)
//        	   {
//        	        		_chunkNumber++;
//        	        		 _chunkPos[2] += 1;
//        	        		 _chunkPos[3] += _skipInterval;
//        	   }
//
//        	if(_cellPos[0] != _lastBucketIdVal)
//        	  {
//        		   _chunkPos[0] += 1;
//        	       _lastBucketIdVal =_cellPos[0];
//        	  }
//
//        	//_chunkPos[2] = 0;
//            //_chunkPos[3] = 0;
//
//            //LOG4CXX_DEBUG(logger, "Chunk0:" << _chunkPos[0] << " ,Chunk1: " <<_chunkPos[1] << ",Chunk2: " <<_chunkPos[2] << ",Chunk3: " <<_chunkPos[3]);
//            //LOG4CXX_DEBUG(logger, "Cell0:" << _cellPos[0] << " ,Cell1: " <<_cellPos[1] << ",Cell2: " <<_cellPos[2] << ",Cell3: " <<_cellPos[3]);
//            //LOG4CXX_DEBUG(logger, "FOO2.5: ");
//            _citers[0]=_aiters[0] ->newChunk(_chunkPos).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE);
//            for(size_t i=1; i<_nAttrs; ++i)
//            {
//                _citers[i] =_aiters[i] ->newChunk(_chunkPos).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE |
//                                                                                 ChunkIterator::NO_EMPTY_CHECK);
//            }
//            //LOG4CXX_DEBUG(logger, "FOO3: ");
//            //_cellPos[0] = _chunkPos[0];
//
//            _cellPos[0] = bucketId;
//            _cellPos[1] = senderInstance;
//            _cellPos[2] = _chunkNumber;
//            _cellPos[3] = _chunkPos[3];
//
//            //LOG4CXX_DEBUG(logger, "Cell0:" << _cellPos[0] << " ,Cell1: " <<_cellPos[1] << ",Cell2: " <<_cellPos[2] << ",Cell3: " <<_cellPos[3]);
//        }
//        //LOG4CXX_DEBUG(logger, "Cell0:" << _cellPos[0] << " ,Cell1: " <<_cellPos[1]);
//        for(size_t i=0; i<_nAttrs; ++i)
//        {
//            _citers[i] ->setPosition(_cellPos);
//        }
//        //LOG4CXX_DEBUG(logger, "FOO4: ");
//    }
//
//    void advance()
//    {
//        ++_cellPos[0];
//        //++ _cellPos[1];
//        //++_cellPos[2];
//        //++_cellPos[3];
//
//        if (_cellPos[0] % _chunkSize == 0)
//        {
//            if (_citers[0])
//            {
//                for(size_t i=0; i<_nAttrs; ++i)
//                {
//                    _citers[i]->flush();
//                }
//            }
//            _chunkPos[0] += _skipInterval;
//            _chunkPos[1] += 1;
//            _chunkPos[2] = 0;
//            _chunkPos[3] = 0;
//
//            //dims.push_back(DimensionDesc("value_id", 0, MAX_COORDINATE, ARRAY_CHUNK_SIZE, 0));
//            //dims.push_back(DimensionDesc("bucket_id", 0, MAX_COORDINATE, 1, 0));
//            //dims.push_back(DimensionDesc("sender_instance", 0, MAX_COORDINATE, 1, 0));
//            //dims.push_back(DimensionDesc("chunk_number", 0, MAX_COORDINATE, 1, 0));
//
//            LOG4CXX_DEBUG(logger, "FOO2.5: ");
//            _citers[0]=_aiters[0] ->newChunk(_chunkPos).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE);
//            for(size_t i=1; i<_nAttrs; ++i)
//            {
//                _citers[i] =_aiters[i] ->newChunk(_chunkPos).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE |
//                                                                                 ChunkIterator::NO_EMPTY_CHECK);
//            }
//            LOG4CXX_DEBUG(logger, "FOO3: ");
//            _cellPos[0] = _chunkPos[0];
//            _cellPos[1] = 0;
//            _cellPos[2] = 0;
//            _cellPos[3] = 0;
//        }
//        for(size_t i=0; i<_nAttrs; ++i)
//        {
//            _citers[i] ->setPosition(_cellPos);
//        }
//        //LOG4CXX_DEBUG(logger, "FOO4: ");
//    }
//
//public:
//    MergeMemArrayAppender(shared_ptr<MemArray> memArray,
//                     shared_ptr<Query> const& query,
//                     size_t startingChunk = 0,
//                     size_t skipInterval  = 1000000):
//       _query(query),
//       _nAttrs(memArray->getArrayDesc().getAttributes(true).size()),
//       _chunkSize(memArray->getArrayDesc().getDimensions()[0].getChunkInterval()),
//       _startingChunk(startingChunk),
//       _skipInterval(skipInterval),
//	   //_cellPos(2,startingChunk - 1),
//	   //_chunkPos(2,startingChunk - skipInterval),
//	   //_cellPos(2),
//	   //_chunkPos(2),
//	   _chunkNumber(0),
//	   _lastBucketIdVal(0),
//	   //_cellPos(1,startingChunk - 1),
//	   //_chunkPos(1,startingChunk - skipInterval),
//       _aiters(_nAttrs),
//       _citers(_nAttrs)
//    {
//
//    	//EXCEPTION_ASSERT(memArray->getArrayDesc().getDimensions().size() == 1 &&
//        //                 memArray->getArrayDesc().getDimensions()[0].getStartMin() == 0 &&
//        //                 skipInterval % memArray->getArrayDesc().getDimensions()[0].getChunkInterval() == 0);
//        for (AttributeID i = 0; i<_nAttrs; ++i)
//        {
//            _aiters[i] = memArray->getIterator(i);
//        }
//        _cellPos.push_back(0);
//        _cellPos.push_back(0);
//        _cellPos.push_back(0);
//        _cellPos.push_back(startingChunk - 1);
//
//        _chunkPos.push_back(0);
//        _chunkPos.push_back(0);
//        _chunkPos.push_back(0);
//        _chunkPos.push_back(startingChunk - skipInterval);
//
//}
//
//    void addValues(Value const& v0, Value const& v1, Value const& v2)
//    {
//        EXCEPTION_ASSERT(_nAttrs==3);
//        advance();
//        _citers[0]->writeItem(v0);
//        _citers[1]->writeItem(v1);
//        _citers[2]->writeItem(v2);
//    }
//
//    void addValues(Value const& v0, uint64_t const v1, uint64_t const v2)
//    {
//        EXCEPTION_ASSERT(_nAttrs==3);
//        advance();
//        _citers[0]->writeItem(v0);
//        _buf.setUint64(v1);
//        _citers[1]->writeItem(_buf);
//        _buf.setUint64(v2);
//        _citers[2]->writeItem(_buf);
//    }
//
//    void addValues( Value const& v0,uint64_t const v1,Coordinate bucketId,Coordinate senderInstance )
//      {
//          EXCEPTION_ASSERT(_nAttrs==2);
//
//	      advance(bucketId, senderInstance);
//
//          _citers[0]->writeItem(v0);
//          _buf.setUint64(v1);
//          _citers[1]->writeItem(_buf);
//
//
//      }
//
//    void addValues(Value const& v0)
//    {
//        EXCEPTION_ASSERT(_nAttrs==1);
//        advance();
//        _citers[0]->writeItem(v0);
//    }
//
//    void release()
//    {
//        if (_citers[0])
//        {
//            for(size_t i=0; i<_nAttrs; ++i)
//            {
//                _citers[i]->flush();
//                _citers[i].reset();
//            }
//        }
//        for(size_t i=0; i<_nAttrs; ++i)
//        {
//            _aiters[i].reset();
//        }
//    }
//};
//
//
//
//class MergeMemArrayBuilder
//{
//private:
//    shared_ptr<MemArray>               _out;
//    MergeMemArrayAppender              _appender;
//
//public:
//    MergeMemArrayBuilder (ArrayDesc const& schema,
//                     shared_ptr<Query> const& query,
//                     size_t startingChunk = 0,
//                     size_t skipInterval  = 1000000):
//        _out(new MemArray(schema, query)),
//        _appender(_out, query, startingChunk, skipInterval)
//    {}
//
//    void addValues(Value const& v0)
//    {
//        _appender.addValues(v0);
//    }
//
//    void addValues(Value const& v0, Value const& v1, Value const& v2)
//    {
//        _appender.addValues(v0,v1,v2);
//    }
//
//    void addValues( Value const& v0, uint64_t const v1, Coordinate bucketId, Coordinate senderInstance )
//     {
//           _appender.addValues(v0,v1, bucketId, senderInstance );
//     }
//
//    void addValues(Value const& v0, uint64_t const v1, uint64_t const v2)
//    {
//        _appender.addValues(v0,v1,v2);
//    }
//
//    shared_ptr<MemArray> finalize()
//    {
//
//    	_appender.release();
//        return _out;
//    }
//    shared_ptr<MemArray> finalizeNoFlush()
//    {
//        return _out;
//    }
//
//};
//
//
//class MemArrayBuilder
//{
//private:
//    shared_ptr<MemArray>               _out;
//    MemArrayAppender                   _appender;
//
//public:
//    MemArrayBuilder (ArrayDesc const& schema,
//                     shared_ptr<Query> const& query,
//                     size_t startingChunk = 0,
//                     size_t skipInterval  = 1000000):
//        _out(new MemArray(schema, query)),
//        _appender(_out, query, startingChunk, skipInterval)
//    {}
//
//   /* MemArrayBuilder (ArrayDesc const& schema,
//                        shared_ptr<Query> const& query,
//                        size_t startingChunk = 0,
//                        size_t skipInterval  = 1000000,
//						shared_ptr<MemArray> inArray ):
//           _out(inArray),
//           _appender(_out, query, startingChunk, skipInterval)
//       {}
//   */
//
//    void addValues(Value const& v0)
//    {
//        _appender.addValues(v0);
//    }
//
//    void addValues(Value const& v0, Value const& v1, Value const& v2)
//    {
//        _appender.addValues(v0,v1,v2);
//    }
//
//    void addValues( Value const& v0, uint64_t const v1)
//     {
//           _appender.addValues(v0,v1);
//     }
//
//    void addValues(Value const& v0, uint64_t const v1, uint64_t const v2)
//    {
//        _appender.addValues(v0,v1,v2);
//    }
//
//    shared_ptr<MemArray> finalize()
//    {
//
//    	_appender.release();
//        return _out;
//    }
//    shared_ptr<MemArray> finalizeNoFlush()
//    {
//        return _out;
//    }
//
//};
//
//
//class ValueChain : private mgd::set<Value, AttributeComparator>
//{
//private:
//    typedef mgd::set<Value, AttributeComparator> super;
//
//public:
//    using super::begin;
//    using super::end;
//    using super::empty;
//    using super::const_iterator;
//
//    ValueChain(AttributeComparator const& comparator):
//        super(comparator)
//    {}
//
//    ValueChain(ArenaPtr const& arena, AttributeComparator const& comparator):
//        super(arena.get(), comparator)
//    {}
//
//    //ValueChain(super::allocator_type const& a, ValueChain const& c):
//    //	super(a, c)
//    //{}
//
//    //ValueChain(mgd::set<Value, AttributeComparator>::allocator_type const& a, BOOST_RV_REF(ValueChain) c):
//    //    super(a, c)
//    //{}
//
//    bool insert(Value const& item)
//    {
//        return super::insert(item).second;
//    }
//};
//
//class HashBucket : private mgd::map<uint64_t, ValueChain >
//{
//private:
//     typedef mgd::map<uint64_t, ValueChain > super;
//
//public:
//    using super::begin;
//    using super::end;
//    using super::empty;
//    using super::const_iterator;
//
//    HashBucket()
//    {}
//
//    HashBucket(ArenaPtr const& arena):
//        super(arena.get())
//    {}
//
//    //HashBucket(super::allocator_type const& a, HashBucket const& c):
//    //    super(a, c)
//    //{}
//
//    //HashBucket(super::allocator_type const& a, BOOST_RV_REF(HashBucket) c):
//    //    super(a, c)
//    //{}
//
//    bool insert(ArenaPtr const& arena, AttributeComparator const& comparator, uint64_t hash, Value const& item)
//    {
//        ValueChain& v = super::insert(std::make_pair(hash, ValueChain(arena, comparator))).first->second;
//        return v.insert(item);
//    }
//};
//
//class MemoryHashTable
//{
//private:
//    mgd::vector <HashBucket>_data;
//    ArenaPtr _arena;
//    mgd::set<size_t> _occupiedBuckets;
//    size_t _numValues;
//    size_t _numUsedBuckets;
//    AttributeComparator const _comparator;
//
//public:
//    static size_t const NUM_BUCKETS      = 1000037;
//
//    MemoryHashTable(AttributeComparator const& comparator, ArenaPtr const& arena):
//        _data(arena.get(), NUM_BUCKETS, HashBucket(arena)),
//        _arena(arena),
//        _occupiedBuckets(arena.get()),
//        _numValues(0),
//        _numUsedBuckets(0),
//        _comparator(comparator)
//    {}
//
//    bool insert(Value const& item)
//    {
//        uint64_t hash = hashValue(item);
//        uint64_t bucketNo = hash % NUM_BUCKETS;
//        HashBucket& bucket = _data[bucketNo];
//        if ( bucket.empty())
//        {
//            _occupiedBuckets.insert(bucketNo);
//            ++_numUsedBuckets;
//        }
//        if(bucket.insert(_arena, _comparator, hash, item))
//        {
//            ++_numValues;
//            return true;
//        }
//        return false;
//    }
//
//    bool empty() const
//    {
//        return _numValues == 0;
//    }
//
//    size_t size() const
//    {
//        return _numValues;
//    }
//
//    size_t usedBytes() const
//    {
//        return _arena->allocated();
//    }
//
//    class const_iterator
//    {
//    private:
//        mgd::vector <HashBucket> const& _data;
//        mgd::set<size_t> const& _occupiedBuckets;
//        mgd::set<size_t>::const_iterator _tableIter;
//        HashBucket::const_iterator _bucketIter;
//        ValueChain::const_iterator _chainIter;
//
//    public:
//        const_iterator(mgd::vector <HashBucket> const& data,
//                       mgd::set<size_t> const& occupiedBuckets):
//          _data(data),
//          _occupiedBuckets(occupiedBuckets)
//        {
//            restart();
//        }
//
//        void restart()
//        {
//            _tableIter = _occupiedBuckets.begin();
//            if(_tableIter != _occupiedBuckets.end())
//            {
//                _bucketIter = _data[ *_tableIter ].begin();
//                _chainIter = _bucketIter->second.begin();
//            }
//        }
//
//        bool end() const
//        {
//            return _tableIter == _occupiedBuckets.end();
//        }
//
//        void next()
//        {
//            if (end())
//            {
//                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "iterating past end";
//            }
//            ++(_chainIter);
//            if (_chainIter == _bucketIter->second.end())
//            {
//                ++(_bucketIter);
//                if(_bucketIter == _data[ *_tableIter ].end() )
//                {
//                    ++(_tableIter);
//                    if(end())
//                    {
//                        return;
//                    }
//                    _bucketIter = _data[ *_tableIter ].begin();
//                }
//                _chainIter = _bucketIter->second.begin();
//            }
//        }
//
//        uint64_t getCurrentBucket() const
//        {
//            if (end())
//            {
//                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
//            }
//            return *_tableIter;
//        }
//
//        uint64_t getCurrentHashMod() const
//        {
//            if (end())
//            {
//                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
//            }
//            return _bucketIter->first % NUM_BUCKETS;
//        }
//
//        uint64_t getCurrentHash() const
//        {
//            if (end())
//            {
//                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
//            }
//            return _bucketIter->first;
//        }
//
//        Value const& getCurrentItem() const
//        {
//            if (end())
//            {
//                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
//            }
//            return (*_chainIter);
//        }
//    };
//
//    const_iterator getIterator() const
//    {
//        return const_iterator(_data, _occupiedBuckets);
//    }
//
//    void dumpStatsToLog() const
//    {
//        LOG4CXX_DEBUG(logger, "Hashtable buckets " << _numUsedBuckets
//                                     << " values " << _numValues
//                                     << " arena " << *(_arena.get()) );
//    }
//
//    shared_ptr<MemArray> dumpToArray(ArrayDesc const& schema, shared_ptr<Query> const& query)
//    {
//        MemArrayBuilder output (schema, query);
//        Value tableItem, hash, hashMod;
//        for (const_iterator tableIter = getIterator(); !tableIter.end(); tableIter.next())
//        {
//            tableItem = tableIter.getCurrentItem();
//            hash.setUint64(tableIter.getCurrentHash());
//            hashMod.setUint64(tableIter.getCurrentHashMod());
//            output.addValues(tableItem, hash, hashMod);
//        }
//
//        return output.finalize();
//    }
//
//    shared_ptr<MemArray> dumpToMergeArray(ArrayDesc const& schema, shared_ptr<Query> const& query, Coordinate nInstances , Coordinate myInstanceId)
//    {
//        MergeMemArrayBuilder output (schema, query);
//        Value tableItem, hashMod;
//        uint64_t hash;
//        for (const_iterator tableIter = getIterator(); !tableIter.end(); tableIter.next())
//        {
//            tableItem = tableIter.getCurrentItem();
//            hash      = tableIter.getCurrentHash();
//            size_t bucketId = hash % nInstances;
//            output.addValues(tableItem, hash, bucketId, myInstanceId);
//        }
//
//
//        return output.finalize();
//    }
//
//    shared_ptr<MemArray> appendToArray(ArrayDesc const& schema, shared_ptr<Query> const& query)
//    {
//        MemArrayBuilder output (schema, query);
//        Value tableItem, hash, hashMod;
//        for (const_iterator tableIter = getIterator(); !tableIter.end(); tableIter.next())
//        {
//            tableItem = tableIter.getCurrentItem();
//            hash.setUint64(tableIter.getCurrentHash());
//            hashMod.setUint64(tableIter.getCurrentHashMod());
//            output.addValues(tableItem, hash, hashMod);
//        }
//        return output.finalize();
//    }
//
//    shared_ptr<MemArray> dumpToArrayNoFlush(ArrayDesc const& schema, shared_ptr<Query> const& query)
//    {
//        MemArrayBuilder output (schema, query);
//        Value tableItem, hash, hashMod;
//        for (const_iterator tableIter = getIterator(); !tableIter.end(); tableIter.next())
//        {
//            tableItem = tableIter.getCurrentItem();
//            hash.setUint64(tableIter.getCurrentHash());
//            hashMod.setUint64(tableIter.getCurrentHashMod());
//            output.addValues(tableItem, hash, hashMod);
//        }
//        return output.finalizeNoFlush();
//    }
//
//    size_t returnSize()
//       {
//           return size();
//       }
//};
//
//class InputDouble
//{
//private:
//    shared_ptr <ConstArrayIterator> _valueAIter;
//    shared_ptr <ConstArrayIterator> _hashAIter;
//    shared_ptr <ConstChunkIterator> _valueCIter;
//    shared_ptr <ConstChunkIterator> _hashCIter;
//    bool _end;
//
//public:
//    InputDouble(shared_ptr<MemArray> & input):
//        _valueAIter  (input->getConstIterator(0)),
//        _hashAIter   (input->getConstIterator(1)),
//        _end  (false)
//    {
//        if(!_valueAIter->end())
//        {
//            _valueCIter   = _valueAIter->getChunk().getConstIterator();
//            _hashCIter     = _hashAIter->getChunk().getConstIterator();
//        }
//        else
//        {
//            _end = true;
//        }
//    }
//
//    virtual ~InputDouble()
//    {
//        unlink();
//    }
//
//    void next()
//    {
//        ++(*_valueCIter);
//        if (!_valueCIter->end())
//        {
//            ++(*_hashCIter);
//        }
//        else
//        {
//            ++(*_valueAIter);
//            if (_valueAIter->end())
//            {
//                _end = true;
//                return;
//            }
//            ++(*_hashAIter);
//            _valueCIter   = _valueAIter->getChunk().getConstIterator();
//            _hashCIter    = _hashAIter->getChunk().getConstIterator();
//        }
//    }
//
//    void getItems(Value& v, uint64_t& hash, uint64_t& hashMod)
//    {
//        v = _valueCIter->getItem();
//        hash = _hashCIter->getItem().getUint64();
//    }
//
//    Coordinates const& getPosition()
//    {
//        return _valueCIter->getPosition();
//    }
//
//    bool end()
//    {
//        return _end;
//    }
//
//    void unlink()
//    {
//        _valueCIter.reset();
//        _hashCIter.reset();
//
//        _valueAIter.reset();
//        _hashAIter.reset();
//
//    }
//};
//
//class InputTriplet
//{
//private:
//    shared_ptr <ConstArrayIterator> _valueAIter;
//    shared_ptr <ConstArrayIterator> _hashAIter;
//    shared_ptr <ConstArrayIterator> _hashModAIter;
//    shared_ptr <ConstChunkIterator> _valueCIter;
//    shared_ptr <ConstChunkIterator> _hashCIter;
//    shared_ptr <ConstChunkIterator> _hashModCIter;
//    bool _end;
//
//public:
//    InputTriplet(shared_ptr<MemArray> & input):
//        _valueAIter  (input->getConstIterator(0)),
//        _hashAIter   (input->getConstIterator(1)),
//        _hashModAIter(input->getConstIterator(2)),
//        _end  (false)
//    {
//        if(!_valueAIter->end())
//        {
//            _valueCIter = _valueAIter->getChunk().getConstIterator();
//            _hashCIter    = _hashAIter->getChunk().getConstIterator();
//            _hashModCIter = _hashModAIter->getChunk().getConstIterator();
//        }
//        else
//        {
//            _end = true;
//        }
//    }
//
//    virtual ~InputTriplet()
//    {
//        unlink();
//    }
//
//    void next()
//    {
//        ++(*_valueCIter);
//        if (!_valueCIter->end())
//        {
//            ++(*_hashCIter);
//            ++(*_hashModCIter);
//        }
//        else
//        {
//            ++(*_valueAIter);
//            if (_valueAIter->end())
//            {
//                _end = true;
//                return;
//            }
//            ++(*_hashAIter);
//            ++(*_hashModAIter);
//            _valueCIter   = _valueAIter->getChunk().getConstIterator();
//            _hashCIter    = _hashAIter->getChunk().getConstIterator();
//            _hashModCIter = _hashModAIter->getChunk().getConstIterator();
//        }
//    }
//
//    void getItems(Value& v, uint64_t& hash, uint64_t& hashMod)
//    {
//        v = _valueCIter->getItem();
//        hash = _hashCIter->getItem().getUint64();
//        hashMod = _hashModCIter->getItem().getUint64();
//    }
//    void getItems(Value& v, uint64_t& hash)
//     {
//    	hash = _hashCIter->getItem().getUint64();
//    	v    = _valueCIter->getItem();
//     }
//
//    Coordinates const& getPosition()
//    {
//        return _valueCIter->getPosition();
//    }
//
//    bool end()
//    {
//        return _end;
//    }
//
//    void unlink()
//    {
//        _valueCIter.reset();
//        _hashCIter.reset();
//        _hashModCIter.reset();
//        _valueAIter.reset();
//        _hashAIter.reset();
//        _hashModAIter.reset();
//    }
//};
//
//
//class HashTableMerger
//{
//private:
//
//
//    InputTriplet        _left,
//                        _right;
//    MemArrayBuilder     _output;
//    AttributeComparator _comparator;
//
//public:
//    HashTableMerger (shared_ptr<Query> const& query,
//                     shared_ptr<MemArray>& left,
//                     shared_ptr<MemArray>& right):
//        _left(left),
//        _right(right),
//        _output(left->getArrayDesc(), query),
//        _comparator(left->getArrayDesc().getAttributes()[0].getType())
//    {}
//
//    shared_ptr <MemArray> doMerge()
//    {
//        uint64_t leftHash, leftHashMod, rightHash, rightHashMod;
//        Value leftItem, rightItem;
//        while(!_left.end() && !_right.end())
//        {
//            _left.getItems (leftItem,  leftHash,  leftHashMod);
//            _right.getItems(rightItem, rightHash, rightHashMod);
//            if(leftHashMod < rightHashMod ||
//               (leftHashMod == rightHashMod && leftHash < rightHash) ||
//               (leftHashMod == rightHashMod && leftHash == rightHash && _comparator(leftItem, rightItem)))
//            {
//                _output.addValues(leftItem, leftHash, leftHashMod);
//                _left.next();
//            }
//            else if(rightHashMod < leftHashMod ||
//                    (rightHashMod == leftHashMod && rightHash < leftHash) ||
//                    (rightHashMod == leftHashMod && rightHash == leftHash && _comparator(rightItem, leftItem)))
//            {
//                _output.addValues(rightItem, rightHash, rightHashMod);
//                _right.next();
//            }
//            else
//            {
//                _output.addValues(rightItem, rightHash, rightHashMod);
//                _left.next();
//                _right.next();
//            }
//        }
//        while(!_left.end())
//        {
//            _left.getItems (leftItem,  leftHash,  leftHashMod);
//            _output.addValues(leftItem, leftHash, leftHashMod);
//            _left.next();
//        }
//        while(!_right.end())
//        {
//            _right.getItems(rightItem, rightHash, rightHashMod);
//            _output.addValues(rightItem, rightHash, rightHashMod);
//            _right.next();
//        }
//        _left.unlink();
//        _right.unlink();
//        return _output.finalize();
//    }
//
//    static shared_ptr<MemArray> merge(shared_ptr<Query> const& query, shared_ptr<MemArray>& left, shared_ptr<MemArray>& right)
//    {
//        HashTableMerger m(query, left, right);
//        return m.doMerge();
//    }
//};
//
//#define MB 1000000
//class SpillingHashCollector
//{
//private:
//    size_t const                   _memoryLimitBytes;
//    size_t const                   _pageSizeBytes;
//    ArenaPtr const&                _parentArena;
//    ArenaPtr                       _arena;
//    ArenaPtr                       _sortArena;
//    AttributeDesc const&           _inputDesc;
//    AttributeComparator            _comparator;
//    shared_ptr<MemoryHashTable>    _memData;
//    vector< shared_ptr<MemArray> > _spilledParts;
//    shared_ptr <Query> const&      _query;
//    shared_ptr<MemArrayBuilder>    _spill;
//    shared_ptr<MemArray>           _sortedArray;
//    //shared_ptr<MemArray>           _finalSpill;
//    shared_ptr<MergeMemArrayBuilder>    _finalSpill;
//
//public:
//    SpillingHashCollector(size_t const memLimitBytes,  ArenaPtr const& parentArena,ArrayDesc const& inputArrayDesc, AttributeDesc const& inputDesc,
//                          shared_ptr<Query> const& query):
//        _memoryLimitBytes(memLimitBytes),
//        _pageSizeBytes(1*MB),
//        _parentArena(parentArena),
//		_arena(newArena(Options("").resetting(true).pagesize(_pageSizeBytes).parent(_parentArena))),
//		_inputDesc(inputDesc),
//        _comparator(_inputDesc.getType()),
//        _memData(new MemoryHashTable(_comparator, _arena)),
//		_query(query),
//        //_overflow( new MemArrayBuilder(inputArrayDesc, query))
//        _spill( new MemArrayBuilder(getSpillMemArrayDesc(), _query)),
//        // _finalSpill(new MemArray(getMergeArrayDesc(), _query))
//        _finalSpill(new MergeMemArrayBuilder(getMergeArrayDesc(), _query))
//
//         //MemArrayBuilder output (schema, query);
//    {}
//
//    void insert(Value const& v)
//    {
//
//    	//_overflow->addValues(v);
//    	if (_memData->usedBytes() <= _memoryLimitBytes)
//    	{
//    		_memData->insert(v);
//    	}
//    	else
//        {
//           // LOG4CXX_DEBUG(logger, "Spilling part with "<<_memData->size()<<" values "<<_memData->usedBytes()<<" bytes into bucket "<<_spilledParts.size());
//            //shared_ptr <MemArray> spill = _memData->dumpToArray(getArrayDesc(), _query);
//            //_memData.reset();
//            //_arena = newArena(Options("").resetting(true).pagesize(_pageSizeBytes).parent(_parentArena));
//            //_memData.reset(new MemoryHashTable(_comparator, _arena));
//            //_spilledParts.push_back(spill);
//
//            /*Value tableItem, hash, hashMod;
//            for (MemoryHashTable::const_iterator tableIter = _memData->getIterator(); !tableIter.end(); tableIter.next())
//            {
//            	tableItem = tableIter.getCurrentItem();
//            	hash.setUint64(tableIter.getCurrentHash());
//            	hashMod.setUint64(tableIter.getCurrentHashMod());
//            	_overflow->addValues(tableItem, hash, hashMod);
//            }
//            */
//    		size_t const nInstances = _query->getInstancesCount();
//    		uint64_t key = hashValue(v);
//            size_t bucketId = key % nInstances;
//            _spill->addValues(v,key,bucketId);
//
//            //_spill->addValues(v);
//        }
//
//    }
//
//    Value& genRandomValue(TypeId const& type, Value& value, int percentNull, int nullReason)
//       {
//           assert(percentNull>=0 && percentNull<=100);
//
//           if (percentNull>0 && rand()%100<percentNull) {
//               value.setNull(nullReason);
//           } else if (type==TID_INT64) {
//               value.setInt64(rand());
//           } else if (type==TID_BOOL) {
//               value.setBool(rand()%100<50);
//           } else if (type==TID_STRING) {
//               vector<char> str;
//               const size_t maxLength = 300;
//               const size_t minLength = 1;
//               assert(minLength>0);
//               size_t length = rand()%(maxLength-minLength) + minLength;
//               str.resize(length + 1);
//               for (size_t i=0; i<length; ++i) {
//                   int c;
//                   do {
//                       c = rand()%128;
//                   } while (! isalnum(c));
//                   str[i] = (char)c;
//               }
//               str[length-1] = 0;
//               value.setString(&str[0]);
//           } else {
//               throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
//                   << "UnitTestSortArrayPhysical" << "genRandomValue";
//           }
//           return value;
//       }
//
//    typedef map<Coordinate, Value> CoordValueMap;
//    typedef std::pair<Coordinate, Value> CoordValueMapEntry;
//
//    void insertMapDataIntoArray(shared_ptr<Query>& query, MemArray& array, CoordValueMap const& m)
//    {
//        Coordinates coord(1);
//        coord[0] = 0;
//        vector< shared_ptr<ArrayIterator> > arrayIters(array.getArrayDesc().getAttributes(true).size());
//        vector< shared_ptr<ChunkIterator> > chunkIters(arrayIters.size());
//
//        for (size_t i = 0; i < arrayIters.size(); i++)
//        {
//            arrayIters[i] = array.getIterator(i);
//            chunkIters[i] =
//                ((MemChunk&)arrayIters[i]->newChunk(coord)).getIterator(query,
//                                                                        ChunkIterator::SEQUENTIAL_WRITE);
//        }
//
//        BOOST_FOREACH(CoordValueMapEntry const& p, m) {
//            coord[0] = p.first;
//            for (size_t i = 0; i < chunkIters.size(); i++)
//            {
//                if (!chunkIters[i]->setPosition(coord))
//                {
//                    chunkIters[i]->flush();
//                    chunkIters[i].reset();
//                    chunkIters[i] =
//                        ((MemChunk&)arrayIters[i]->newChunk(coord)).getIterator(query,
//                                                                                ChunkIterator::SEQUENTIAL_WRITE);
//                    chunkIters[i]->setPosition(coord);
//                }
//                chunkIters[i]->writeItem(p.second);
//            }
//        }
//
//        for (size_t i = 0; i < chunkIters.size(); i++)
//        {
//            chunkIters[i]->flush();
//        }
//    }
//
//    shared_ptr <MemArray> finalizeSort()
//
//    {
//    	        // Need to sort on <key, value>[i]
//    	        LOG4CXX_DEBUG(logger, "Entering the sort of the spill data");
//
//                // Sort Keys
//    	        SortingAttributeInfos sortingAttributeInfos;
//    	        SortingAttributeInfo  k;
//    	        //k.columnNo = 0;
//    	        //k.ascent = true;
//    	        //sortingAttributeInfos.push_back(k);
//    	        k.columnNo = 2;
//    	        k.ascent = true;
//    	        sortingAttributeInfos.push_back(k);
//
//    	        shared_ptr<MemArray> arrayInst = _spill->finalize();
//
//    	        shared_ptr<Array> baseArrayInst = static_pointer_cast<MemArray, Array>(arrayInst);
//                shared_ptr<Query> query = _query;
//                ArrayDesc schema = getSpillMemArrayDesc();
//
//                //attrs.push_back(AttributeDesc(1, "key",  TID_UINT64, 0, 0));
//                //attrs.push_back(AttributeDesc(2, "bucketid",  TID_UINT64, 0, 0));
//                //dims.push_back(DimensionDesc("ii", 0, MAX_COORDINATE, ARRAY_CHUNK_SIZE, 0));
//
//                // Sort
//    	        const bool preservePositions = false;
//    	        SortArray sorter(schema, _arena, preservePositions);
//
//    	        shared_ptr<TupleComparator> tcomp(new TupleComparator(sortingAttributeInfos, schema));
//
//    	        //PhysicalOperator::dumpArrayToLog(baseArrayInst, logger);
//    	        _sortedArray = sorter.getSortedArray(baseArrayInst, _query, tcomp);
//    	        //PhysicalOperator::dumpArrayToLog(baseArrayInst, logger);
//    	        //PhysicalOperator::dumpArrayToLog(_sortedArray, logger);
//
//    	    	//shared_ptr<MemArray> finalizedSort( new MemArray(getMergeArrayDesc(), _query));
//    	        size_t const nInstances       = _query->getInstancesCount();
//    	        InstanceID const myInstanceId = _query->getInstanceID();
//
//    	        LOG4CXX_DEBUG(logger, "Appending FOO1: ");
//    	        // MergeMemArrayAppender appenderFinal =MergeMemArrayAppender(_finalSpill,query, -1 ,1000000 );
//    	        vector <shared_ptr<MergeMemArrayAppender> > appenders(1);
//    	        //appenders[0].reset(new MergeMemArrayAppender(_finalSpill, _query,0,1000000));
//    	        //appenders[i].reset(new MemArrayAppender(output, _query, startingChunk, skipInterval));
//    	        InputTriplet inputReader(_sortedArray);
//    	        Value val;
//    	        uint64_t hash, hashMod, bucketId;
//
//    	       // inputReader.getItems(hash, v);
//    	       //size_t destinationInstance = hash % nInstances;
//    	       //PhysicalOperator::dumpArrayToLog(_sortedArray, logger);
//    	       string ref="";
//
//    	       //decompress the data.
//    	       while(!inputReader.end())
//    	        {
//    	            inputReader.getItems(val,hash,bucketId);
//    	            string(val.getString());
//
//    	            if(strcmp(ref.c_str(),string(val.getString()).c_str() )!=0)
//    	              {
//    	                ref = string(val.getString());
//    	                //uniqueVals++;
//    	                //add the data to a new array specified for merging.
//    	                _finalSpill->addValues(val,hash,bucketId,myInstanceId);
//    	              }
//    	         inputReader.next();
//    	        }
//
//    	       LOG4CXX_DEBUG(logger, "Finished decompressing data: ");
//
//    	       //appenders[0]->release();
//    	       shared_ptr<MemArray> out = _finalSpill->finalize();
//
//    	       //PhysicalOperator::dumpArrayToLog(out, logger);
//    	       LOG4CXX_DEBUG(logger, "After Dump Array: ");
//    	        //appenders[0]->release();
//
//    	        //inputReader.unlink();
//    	        //finalizedArray.reset();
//
//    	        return out;
//
//    	        /*
//    	        shared_ptr<ConstArrayIterator> memArrayIter(outputMemArray->getConstIterator(0));  //everyone has an attribute 0! Even the strangest arrays...
//    	                shared_ptr<ConstChunkIterator> memChunkIter;
//
//    	                size_t numMemChunks = 0;
//    	                size_t numMemCells  = 0;
//    	                size_t uniqueVals   = 0;
//
//    	                string ref="";
//    	                while (!memArrayIter->end())
//    	                {
//    	                    ++numMemChunks;
//    	                    memChunkIter = memArrayIter->getChunk().getConstIterator();
//    	                    while(! memChunkIter->end())
//    	                    {
//
//    	                    	Value const& val = memChunkIter->getItem();
//
//    	                    	string(val.getString());
//
//    	                    	if(strcmp(ref.c_str(),string(val.getString()).c_str() )!=0)
//    	                    	{
//
//    	                    		ref=string(val.getString());
//    	                    		uniqueVals++;
//    	                    	}
//
//    	                    	++numMemCells;
//    	                        ++(*memChunkIter);
//    	                    }
//    	                    ++(*memArrayIter);
//    	                }
//*/
//
//    	        //return _sortedArray;
//    }
//
//
//    shared_ptr <MemArray> finalizeHash()
//      {
//
//    	size_t const nInstances = _query->getInstancesCount();
//    	InstanceID const myInstanceId = _query->getInstanceID();
//
//    	shared_ptr <MemArray> finalizedHash = _memData->dumpToMergeArray(getMergeArrayDesc(), _query, nInstances, myInstanceId);
//
//    	return finalizedHash;
//    	//shared_ptr<MemArray> dumpToMergeArray(ArrayDesc const& schema, shared_ptr<Query> const& query, Coordinate bucketId, Coordinate myInstanceId)
//      }
//
//    size_t returnSize()
//    {
//    	size_t result = _memData->size();
//		return result;
//    }
//
//    size_t releaseMemory()
//      {
//    	 _memData.reset();
//      }
//
//    ArrayDesc getMergeArrayDesc()
//       {
//           Attributes attrs;
//           attrs.push_back(AttributeDesc(0,
//                                         _inputDesc.getName(),
//                                         _inputDesc.getType(),
//                                         0,  //no longer nullable!
//                                         _inputDesc.getDefaultCompressionMethod(),
//                                         _inputDesc.getAliases(),
//                                         _inputDesc.getReserve(),
//                                         &(_inputDesc.getDefaultValue()),
//                                         _inputDesc.getDefaultValueExpr(),
//                                         _inputDesc.getVarSize()));
//
//           attrs.push_back(AttributeDesc(1, "key",    TID_UINT64, 0, 0));
//           //attrs.push_back(AttributeDesc(1, "value",    TID_UINT64, 0, 0));
//           attrs = addEmptyTagAttribute(attrs);
//           // param name dimension name
//           // start dimension start
//           // param end dimension end
//           // chunkInterval chunk size in this dimension
//           // chunkOverlap chunk overlay in this dimension
//
//           Dimensions dims;
//           dims.push_back(DimensionDesc("bucket_id", 0, MAX_COORDINATE, 1, 0));
//           //dims.push_back(DimensionDesc("instance_id", 0, MAX_COORDINATE, 1, 0));
//           dims.push_back(DimensionDesc("sender_instance", 0, MAX_COORDINATE, 1, 0));
//           dims.push_back(DimensionDesc("chunk_number", 0, MAX_COORDINATE, 1, 0));
//           dims.push_back(DimensionDesc("value_id", 0, MAX_COORDINATE, ARRAY_CHUNK_SIZE, 0));
//
//           return ArrayDesc("mergearr", attrs, dims);
//       }
//
//    ArrayDesc getArrayDesc()
//    {
//        Attributes attrs;
//        attrs.push_back(AttributeDesc(0,
//                                      _inputDesc.getName(),
//                                      _inputDesc.getType(),
//                                      0,  //no longer nullable!
//                                      _inputDesc.getDefaultCompressionMethod(),
//                                      _inputDesc.getAliases(),
//                                      _inputDesc.getReserve(),
//                                      &(_inputDesc.getDefaultValue()),
//                                      _inputDesc.getDefaultValueExpr(),
//                                      _inputDesc.getVarSize()));
//        attrs.push_back(AttributeDesc(1, "hash",    TID_UINT64, 0, 0));
//        attrs.push_back(AttributeDesc(2, "hashmod", TID_UINT64, 0, 0));
//        attrs = addEmptyTagAttribute(attrs);
//        Dimensions dims;
//        dims.push_back(DimensionDesc("dim", 0, MAX_COORDINATE, ARRAY_CHUNK_SIZE, 0));
//        return ArrayDesc("arr", attrs, dims);
//    }
//
//    ArrayDesc getSpillMemArrayDesc()
//    {
//        Attributes attrs;
//
//        attrs.push_back(AttributeDesc(0,
//                                      _inputDesc.getName(),
//                                      _inputDesc.getType(),
//                                      0,  //no longer nullable!
//                                      _inputDesc.getDefaultCompressionMethod(),
//                                      _inputDesc.getAliases(),
//                                      _inputDesc.getReserve(),
//                                      &(_inputDesc.getDefaultValue()),
//                                      _inputDesc.getDefaultValueExpr(),
//                                      _inputDesc.getVarSize()));
//
//        //attrs.push_back(AttributeDesc(0, "key",    TID_STRING, 0, 0));
//        attrs.push_back(AttributeDesc(1, "key",  TID_UINT64, 0, 0));
//        attrs.push_back(AttributeDesc(2, "bucketid",  TID_UINT64, 0, 0));
//        attrs = addEmptyTagAttribute(attrs);
//        Dimensions dims;
//
//        dims.push_back(DimensionDesc("ii", 0, MAX_COORDINATE, ARRAY_CHUNK_SIZE, 0));
///*
//        dims.push_back(DimensionDesc("id_number", 0, MAX_COORDINATE, ARRAY_CHUNK_SIZE, 0));
//        dims.push_back(DimensionDesc("bucket_id", 0, MAX_COORDINATE, 1, 0));
//        dims.push_back(DimensionDesc("sender_instance", 0, MAX_COORDINATE, 1, 0));
//        dims.push_back(DimensionDesc("chunk_number", 0, MAX_COORDINATE, 1, 0));
//*/
//
//        return ArrayDesc("spillarr", attrs, dims);
//    }
//
//    static void senderToChunk (InstanceID const senderInstanceId,
//                               InstanceID const targetInstanceId,
//                               size_t const nInstances,
//                               size_t& startingChunk,
//                               size_t& skipInterval,
//                               size_t const chunkSize = 1000000)
//    {
//        skipInterval =  nInstances * chunkSize;
//        startingChunk = nInstances * chunkSize * senderInstanceId + targetInstanceId * chunkSize;
//        LOG4CXX_DEBUG(logger, "Sender "<<senderInstanceId<<" target "<<targetInstanceId << " startingChunk "<<startingChunk << " skipInterval "<<skipInterval);
//    }
//
//    static void chunkToSender (Coordinate chunkPos,
//                               size_t const nInstances,
//                               InstanceID const myInstanceId,
//                               InstanceID& senderInstanceId,
//                               size_t const chunkSize = 1000000)
//    {
//        senderInstanceId = ((chunkPos - chunkSize * myInstanceId) / (nInstances * chunkSize));
//    }
//
//
//    shared_ptr <MemArray> finalizeMerge(shared_ptr <MemArray>& hashArray, shared_ptr <MemArray>& overflowArray, ArrayDesc const& opSchema)
//    {
//
//    	//vector< boost::shared_ptr<Array> >& inputArrays, boost::shared_ptr<Query> query
//    	//assert(inputArrays.size() >= 2);
//    	//        return boost::shared_ptr<Array>(new MergeArray(_schema, inputArrays));
//
//    	LOG4CXX_DEBUG(logger, "finalizeMerge Begin: ");
//
//    	shared_ptr<MemArray> output( new MemArray(getMergeArrayDesc(), _query));
//        size_t const nInstances = _query->getInstancesCount();
//        InstanceID const myInstanceId = _query->getInstanceID();
//        vector <shared_ptr<MergeMemArrayAppender> > appenders(1);
//
//        size_t startingChunk;
//        size_t skipInterval;
//            //senderToChunk(myInstanceId, i, nInstances, startingChunk, skipInterval);
//        appenders[0].reset(new MergeMemArrayAppender(output, _query));
//
//        //LOG4CXX_DEBUG(logger, "FOO1: ");
//
//        Value val;
//        uint64_t hash, hashMod,bucketId;
//
//        InputTriplet inputReaderOverflow(overflowArray);
//        LOG4CXX_DEBUG(logger, "InputTriplet inputReaderOverflow(overflowArray);");
//        while(!inputReaderOverflow.end())
//        {
//
//        	LOG4CXX_DEBUG(logger, "1111111111111 ");
//        	inputReaderOverflow.getItems(val,hash,bucketId);
//        	LOG4CXX_DEBUG(logger, "222222222222 ");
//        	appenders[0]->addValues(val,hash,bucketId,myInstanceId);
//        	LOG4CXX_DEBUG(logger, "33333333333 ");
//        	inputReaderOverflow.next();
//        }
//
//        LOG4CXX_DEBUG(logger, " InputTriplet inputReaderHash(hashArray); ");
//
//        InputTriplet inputReaderHash(hashArray);
//
//        while(!inputReaderHash.end())
//         {
//
//             inputReaderHash.getItems(val,hash,bucketId);
//             //appenders[0]->addValues(val,hash,bucketId,myInstanceId);
//             inputReaderHash.next();
//         }
//        LOG4CXX_DEBUG(logger, " InputTriplet inputReaderHash(hashArray);Out of loop ");
//
//        return output;
//        /*
//        appenders[0]->release();
//
//
//
//        inputReaderHash.unlink();
//        appenders.clear();
//        appenders[0].reset(new MergeMemArrayAppender(output, _query));
//        //finalizedArray.reset();
//
//        output = dynamic_pointer_cast<MemArray>(redistribute(output, _query, psByRow));
//
//
//        PhysicalOperator::dumpArrayToLog(output, logger);
//        LOG4CXX_DEBUG(logger, "Finished redistributing: ");
//
//        InputTriplet inputReaderOutput(output);
//
//        while(!inputReaderOutput.end())
//             {
//                   inputReaderOutput.getItems(val,hash,bucketId);
//                   appenders[0]->addValues(val,hash,bucketId,myInstanceId);
//                   inputReaderOutput.next();
//              }
//
//               appenders[0]->release();
//
//               inputReaderOutput.unlink();
//               appenders.clear();
//*/
//
//    }
//
//
//
//
//
//    shared_ptr <MemArray> makeExchangeArray(shared_ptr <MemArray>& finalizedArray, ArrayDesc const& opSchema)
//    {
//        shared_ptr<MemArray> output( new MemArray(getArrayDesc(), _query));
//        size_t const nInstances = _query->getInstancesCount();
//        InstanceID const myInstanceId = _query->getInstanceID();
//        vector <shared_ptr<MemArrayAppender> > appenders(nInstances);
//        for(InstanceID i=0; i<nInstances; ++i)
//        {
//            size_t startingChunk;
//            size_t skipInterval;
//            senderToChunk(myInstanceId, i, nInstances, startingChunk, skipInterval);
//            appenders[i].reset(new MemArrayAppender(output, _query, startingChunk, skipInterval));
//        }
//        LOG4CXX_DEBUG(logger, "FOO1: ");
//
//        InputTriplet inputReader(finalizedArray);
//        Value v;
//        uint64_t hash, hashMod;
//        while(!inputReader.end())
//        {
//            inputReader.getItems(v, hash, hashMod);
//            size_t destinationInstance = hash % nInstances;
//            appenders[destinationInstance]->addValues(v, hash, hashMod);
//            inputReader.next();
//        }
//        LOG4CXX_DEBUG(logger, "FOO2: ");
//
//        for(InstanceID i=0; i<nInstances; ++i)
//        {
//            appenders[i]->release();
//        }
//        inputReader.unlink();
//        appenders.clear();
//        finalizedArray.reset();
//        //PhysicalOperator::dumpArrayToLog(output, logger);
//        output = dynamic_pointer_cast<MemArray>(redistribute(output, _query, psHashPartitioned));
//        //PhysicalOperator::dumpArrayToLog(output, logger);
//        vector<shared_ptr<MemArrayBuilder> > builders(nInstances);
//        InputTriplet inputReader2(output);
//        while(!inputReader2.end())
//        {
//            inputReader2.getItems(v, hash, hashMod);
//            Coordinate chunkPos = ((inputReader2.getPosition()[0]) / 1000000) * 1000000;
//            size_t senderInstanceId;
//            chunkToSender(chunkPos, nInstances, myInstanceId, senderInstanceId);
//            if (!builders[senderInstanceId])
//            {
//                builders[senderInstanceId].reset(new MemArrayBuilder(getArrayDesc(), _query));
//            }
//            builders[senderInstanceId]->addValues(v, hash, hashMod);
//            inputReader2.next();
//        }
//
//        LOG4CXX_DEBUG(logger, "FOO3: ");
//        inputReader2.unlink();
//        output.reset();
//        vector<shared_ptr<MemArray> > pieces;
//        for(InstanceID i=0; i<nInstances; ++i)
//        {
//            if (builders[i])
//            {
//                //pieces.push_back(builders[i]->finalize());
//                pieces.push_back(builders[i]->finalizeNoFlush());
//                builders[i].reset();
//            }
//        }
//        size_t numPieces = pieces.size();
//        if (numPieces == 0)
//        {
//            return shared_ptr<MemArray> (new MemArray(opSchema, _query));
//        }
//        LOG4CXX_DEBUG(logger, "FOO4: ");
//        while (numPieces > 1)
//        {
//            vector< shared_ptr<MemArray> > newPieces;
//            shared_ptr <MemArray> left;
//            for(size_t i=0; i<numPieces; ++i)
//            {
//                shared_ptr<MemArray> part = pieces[i];
//                if(!left)
//                {
//                    left = part;
//                    if (i == numPieces - 1)
//                    {
//                        newPieces.push_back(left);
//                    }
//                }
//                else
//                {
//                    LOG4CXX_DEBUG(logger, "Merging buckets "<<i-1<<" and "<<i<<" into "<<newPieces.size());
//                    newPieces.push_back(HashTableMerger::merge(_query, left, part));
//                    left.reset();
//                }
//            }
//            numPieces = newPieces.size();
//            pieces = newPieces;
//        }
//        LOG4CXX_DEBUG(logger, "FOO5: ");
//
//        shared_ptr <MemArray> result = pieces[0];
//        pieces.clear();
//        MemArrayBuilder finalResult(opSchema, _query, myInstanceId * 1000000, nInstances * 1000000);
//        InputTriplet inputReader3(result);
//        while(!inputReader3.end())
//        {
//            inputReader3.getItems(v, hash, hashMod);
//            finalResult.addValues(v);
//            inputReader3.next();
//        }
//        inputReader3.unlink();
//        result.reset();
//        return finalResult.finalize();
//    }
//
//};



} //namespace scidb
