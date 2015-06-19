#include <query/Operator.h>
#include <query/AttributeComparator.h>
#include <util/arena/Set.h>
#include <util/arena/Map.h>
#include <util/arena/Vector.h>
#include <util/Arena.h>


namespace scidb
{

using scidb::arena::Options;
using scidb::arena::ArenaPtr;
using scidb::arena::newArena;
using boost::dynamic_pointer_cast;


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

class MemArrayBuilder
{
private:
    shared_ptr<MemArray>               _out;
    MemArrayAppender                   _appender;

public:
    MemArrayBuilder (ArrayDesc const& schema,
                     shared_ptr<Query> const& query,
                     size_t startingChunk = 0,
                     size_t skipInterval  = 1000000):
        _out(new MemArray(schema, query)),
        _appender(_out, query, startingChunk, skipInterval)
    {}

    void addValues(Value const& v0)
    {
        _appender.addValues(v0);
    }

    void addValues(Value const& v0, Value const& v1, Value const& v2)
    {
        _appender.addValues(v0,v1,v2);
    }

    void addValues(Value const& v0, uint64_t const v1, uint64_t const v2)
    {
        _appender.addValues(v0,v1,v2);
    }

    shared_ptr<MemArray> finalize()
    {
        _appender.release();
        return _out;
    }
    shared_ptr<MemArray> finalizeNoFlush()
    {
        return _out;
    }

};
class ValueChain : private mgd::set<Value, AttributeComparator>
{
private:
    typedef mgd::set<Value, AttributeComparator> super;

public:
    using super::begin;
    using super::end;
    using super::empty;
    using super::const_iterator;

    ValueChain(AttributeComparator const& comparator):
        super(comparator)
    {}

    ValueChain(ArenaPtr const& arena, AttributeComparator const& comparator):
        super(arena.get(), comparator)
    {}

   // ValueChain(super::allocator_type const& a, ValueChain const& c):
   // ValueChain(super::allocator_type const& a, ValueChain c):
   // 	super(a, c)
   // {}

    //ValueChain(mgd::set<Value, AttributeComparator>::allocator_type const& a, BOOST_RV_REF(ValueChain) c):
    //    super(a, c)
    //{}

    bool insert(Value const& item)
    {
        return super::insert(item).second;
    }
};

class HashBucket : private mgd::map<uint64_t, ValueChain >
{
private:
     typedef mgd::map<uint64_t, ValueChain > super;

public:
    using super::begin;
    using super::end;
    using super::empty;
    using super::const_iterator;

    HashBucket()
    {}

    HashBucket(ArenaPtr const& arena):
        super(arena.get())
    {}

    //HashBucket(super::allocator_type const& a, HashBucket const& c):
    //    super(a, c)
    //{}

    //HashBucket(super::allocator_type const& a, BOOST_RV_REF(HashBucket) c):
    //    super(a, c)
    //{}

    bool insert(ArenaPtr const& arena, AttributeComparator const& comparator, uint64_t hash, Value const& item)
    {
        ValueChain& v = super::insert(std::make_pair(hash, ValueChain(arena, comparator))).first->second;
        return v.insert(item);
    }
};

class MemoryHashTable
{
private:
    mgd::vector <HashBucket>_data;
    ArenaPtr _arena;
    mgd::set<size_t> _occupiedBuckets;
    size_t _numValues;
    size_t _numUsedBuckets;
    AttributeComparator const _comparator;

public:
    static size_t const NUM_BUCKETS      = 1000037;

    MemoryHashTable(AttributeComparator const& comparator, ArenaPtr const& arena):
        _data(arena.get(), NUM_BUCKETS, HashBucket(arena)),
        _arena(arena),
        _occupiedBuckets(arena.get()),
        _numValues(0),
        _numUsedBuckets(0),
        _comparator(comparator)
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
            return true;
        }
        return false;
    }

    bool empty() const
    {
        return _numValues == 0;
    }

    size_t size() const
    {
        return _numValues;
    }

    size_t usedBytes() const
    {
        return _arena->allocated();
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

        uint64_t getCurrentHashMod() const
        {
            if (end())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "access past end";
            }
            return _bucketIter->first % NUM_BUCKETS;
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
                                     << " arena " << *(_arena.get()) );
    }

    shared_ptr<MemArray> dumpToArray(ArrayDesc const& schema, shared_ptr<Query> const& query)
    {
        MemArrayBuilder output (schema, query);
        Value tableItem, hash, hashMod;
        for (const_iterator tableIter = getIterator(); !tableIter.end(); tableIter.next())
        {
            tableItem = tableIter.getCurrentItem();
            hash.setUint64(tableIter.getCurrentHash());
            hashMod.setUint64(tableIter.getCurrentHashMod());
            output.addValues(tableItem, hash, hashMod);
        }
        return output.finalize();
    }
    shared_ptr<MemArray> dumpToArrayNoFlush(ArrayDesc const& schema, shared_ptr<Query> const& query)
    {
        MemArrayBuilder output (schema, query);
        Value tableItem, hash, hashMod;
        for (const_iterator tableIter = getIterator(); !tableIter.end(); tableIter.next())
        {
            tableItem = tableIter.getCurrentItem();
            hash.setUint64(tableIter.getCurrentHash());
            hashMod.setUint64(tableIter.getCurrentHashMod());
            output.addValues(tableItem, hash, hashMod);
        }
        return output.finalizeNoFlush();
    }

    size_t returnSize()
       {
           return size();
       }
};

class InputTriplet
{
private:
    shared_ptr <ConstArrayIterator> _valueAIter;
    shared_ptr <ConstArrayIterator> _hashAIter;
    shared_ptr <ConstArrayIterator> _hashModAIter;
    shared_ptr <ConstChunkIterator> _valueCIter;
    shared_ptr <ConstChunkIterator> _hashCIter;
    shared_ptr <ConstChunkIterator> _hashModCIter;
    bool _end;

public:
    InputTriplet(shared_ptr<MemArray> & input):
        _valueAIter  (input->getConstIterator(0)),
        _hashAIter   (input->getConstIterator(1)),
        _hashModAIter(input->getConstIterator(2)),
        _end  (false)
    {
        if(!_valueAIter->end())
        {
            _valueCIter = _valueAIter->getChunk().getConstIterator();
            _hashCIter    = _hashAIter->getChunk().getConstIterator();
            _hashModCIter = _hashModAIter->getChunk().getConstIterator();
        }
        else
        {
            _end = true;
        }
    }

    virtual ~InputTriplet()
    {
        unlink();
    }

    void next()
    {
        ++(*_valueCIter);
        if (!_valueCIter->end())
        {
            ++(*_hashCIter);
            ++(*_hashModCIter);
        }
        else
        {
            ++(*_valueAIter);
            if (_valueAIter->end())
            {
                _end = true;
                return;
            }
            ++(*_hashAIter);
            ++(*_hashModAIter);
            _valueCIter   = _valueAIter->getChunk().getConstIterator();
            _hashCIter    = _hashAIter->getChunk().getConstIterator();
            _hashModCIter = _hashModAIter->getChunk().getConstIterator();
        }
    }

    void getItems(Value& v, uint64_t& hash, uint64_t& hashMod)
    {
        v = _valueCIter->getItem();
        hash = _hashCIter->getItem().getUint64();
        hashMod = _hashModCIter->getItem().getUint64();
    }

    Coordinates const& getPosition()
    {
        return _valueCIter->getPosition();
    }

    bool end()
    {
        return _end;
    }

    void unlink()
    {
        _valueCIter.reset();
        _hashCIter.reset();
        _hashModCIter.reset();
        _valueAIter.reset();
        _hashAIter.reset();
        _hashModAIter.reset();
    }
};


class HashTableMerger
{
private:


    InputTriplet        _left,
                        _right;
    MemArrayBuilder     _output;
    AttributeComparator _comparator;

public:
    HashTableMerger (shared_ptr<Query> const& query,
                     shared_ptr<MemArray>& left,
                     shared_ptr<MemArray>& right):
        _left(left),
        _right(right),
        _output(left->getArrayDesc(), query),
        _comparator(left->getArrayDesc().getAttributes()[0].getType())
    {}

    shared_ptr <MemArray> doMerge()
    {
        uint64_t leftHash, leftHashMod, rightHash, rightHashMod;
        Value leftItem, rightItem;
        while(!_left.end() && !_right.end())
        {
            _left.getItems (leftItem,  leftHash,  leftHashMod);
            _right.getItems(rightItem, rightHash, rightHashMod);
            if(leftHashMod < rightHashMod ||
               (leftHashMod == rightHashMod && leftHash < rightHash) ||
               (leftHashMod == rightHashMod && leftHash == rightHash && _comparator(leftItem, rightItem)))
            {
                _output.addValues(leftItem, leftHash, leftHashMod);
                _left.next();
            }
            else if(rightHashMod < leftHashMod ||
                    (rightHashMod == leftHashMod && rightHash < leftHash) ||
                    (rightHashMod == leftHashMod && rightHash == leftHash && _comparator(rightItem, leftItem)))
            {
                _output.addValues(rightItem, rightHash, rightHashMod);
                _right.next();
            }
            else
            {
                _output.addValues(rightItem, rightHash, rightHashMod);
                _left.next();
                _right.next();
            }
        }
        while(!_left.end())
        {
            _left.getItems (leftItem,  leftHash,  leftHashMod);
            _output.addValues(leftItem, leftHash, leftHashMod);
            _left.next();
        }
        while(!_right.end())
        {
            _right.getItems(rightItem, rightHash, rightHashMod);
            _output.addValues(rightItem, rightHash, rightHashMod);
            _right.next();
        }
        _left.unlink();
        _right.unlink();
        return _output.finalize();
    }

    static shared_ptr<MemArray> merge(shared_ptr<Query> const& query, shared_ptr<MemArray>& left, shared_ptr<MemArray>& right)
    {
        HashTableMerger m(query, left, right);
        return m.doMerge();
    }
};

#define MB 1000000
class SpillingHashCollector
{
private:
    size_t const                   _memoryLimitBytes;
    size_t const                   _pageSizeBytes;
    ArenaPtr const&                _parentArena;
    ArenaPtr                       _arena;
    AttributeDesc const&           _inputDesc;
    AttributeComparator            _comparator;
    shared_ptr<MemoryHashTable>    _memData;
    vector< shared_ptr<MemArray> > _spilledParts;
    shared_ptr <Query> const&      _query;

public:
    SpillingHashCollector(size_t const memLimitBytes,  ArenaPtr const& parentArena, AttributeDesc const& inputDesc,
                          shared_ptr<Query> const& query):
        _memoryLimitBytes(memLimitBytes),
        _pageSizeBytes(1*MB),
        _parentArena(parentArena),
        _arena(newArena(Options("").resetting(true).pagesize(_pageSizeBytes).parent(_parentArena))),
        _inputDesc(inputDesc),
        _comparator(_inputDesc.getType()),
        _memData(new MemoryHashTable(_comparator, _arena)),
        _query(query)
    {}

    void insert(Value const& v)
    {
        _memData->insert(v);
        //We don't need to spill yet and this will be redefined as it is not efficient - JR
        /*if (_memData->usedBytes() >= _memoryLimitBytes)
        {
            LOG4CXX_DEBUG(logger, "Spilling part with "<<_memData->size()<<" values "<<_memData->usedBytes()<<" bytes into bucket "<<_spilledParts.size());
            shared_ptr <MemArray> spill = _memData->dumpToArray(getArrayDesc(), _query);
            _memData.reset();
            _arena = newArena(Options("").resetting(true).pagesize(_pageSizeBytes).parent(_parentArena));
            _memData.reset(new MemoryHashTable(_comparator, _arena));
            _spilledParts.push_back(spill);
        }
        */
    }

    shared_ptr <MemArray> finalize()
    {

    	/*if (_spilledParts.size() == 0 && _memData->empty())
        {
            return shared_ptr<MemArray> (new MemArray(getArrayDesc(), _query));
        }
        if (!_memData->empty())// if the memData is not empty, dump to disk?? JR
        {
            shared_ptr <MemArray> spill = _memData->dumpToArray(getArrayDesc(), _query);
            _memData.reset();
            _arena.reset();
            _spilledParts.push_back(spill);
        }
        size_t numParts = _spilledParts.size();
        while (numParts > 1)
        {
            vector< shared_ptr<MemArray> > newParts;
            shared_ptr <MemArray> left;
            for(size_t i=0; i<numParts; ++i)
            {
                shared_ptr<MemArray> part = _spilledParts[i];
                if(!left)
                {
                    left = part;
                    if (i == numParts - 1)
                    {
                        newParts.push_back(left);
                    }
                }
                else
                {
                    LOG4CXX_DEBUG(logger, "Merging buckets "<<i-1<<" and "<<i<<" into "<<newParts.size());
                    newParts.push_back(HashTableMerger::merge(_query, left, part));
                    left.reset();
                }
            }
            numParts = newParts.size();
            _spilledParts = newParts;
        }
        shared_ptr <MemArray> result = _spilledParts[0];
        _spilledParts.clear();
        */


    	shared_ptr <MemArray> result  = _memData->dumpToArray(getArrayDesc(), _query);// Added to bypass the spilling for now - JR
    	//shared_ptr <MemArray> result  = _memData->dumpToArrayNoFlush(getArrayDesc(), _query);
    	//shared_ptr <MemArray> result;
    	return result;

    }
    size_t returnSize()
    {

    	size_t result = _memData->size();
		return result;

    }

    size_t releaseMemory()
      {

    	 _memData.reset();


      }

    ArrayDesc getArrayDesc()
    {
        Attributes attrs;
        attrs.push_back(AttributeDesc(0,
                                      _inputDesc.getName(),
                                      _inputDesc.getType(),
                                      0,  //no longer nullable!
                                      _inputDesc.getDefaultCompressionMethod(),
                                      _inputDesc.getAliases(),
                                      _inputDesc.getReserve(),
                                      &(_inputDesc.getDefaultValue()),
                                      _inputDesc.getDefaultValueExpr(),
                                      _inputDesc.getVarSize()));
        attrs.push_back(AttributeDesc(1, "hash",    TID_UINT64, 0, 0));
        attrs.push_back(AttributeDesc(2, "hashmod", TID_UINT64, 0, 0));
        attrs = addEmptyTagAttribute(attrs);
        Dimensions dims;
        dims.push_back(DimensionDesc("dim", 0, MAX_COORDINATE, ARRAY_CHUNK_SIZE, 0));
        return ArrayDesc("arr", attrs, dims);
    }

    static void senderToChunk (InstanceID const senderInstanceId,
                               InstanceID const targetInstanceId,
                               size_t const nInstances,
                               size_t& startingChunk,
                               size_t& skipInterval,
                               size_t const chunkSize = 1000000)
    {
        skipInterval = nInstances * nInstances * chunkSize;
        startingChunk = nInstances * chunkSize * senderInstanceId + targetInstanceId * chunkSize;
        LOG4CXX_DEBUG(logger, "Sender "<<senderInstanceId<<" target "<<targetInstanceId << " startingChunk "<<startingChunk << " skipInterval "<<skipInterval);
    }

    static void chunkToSender (Coordinate chunkPos,
                               size_t const nInstances,
                               InstanceID const myInstanceId,
                               InstanceID& senderInstanceId,
                               size_t const chunkSize = 1000000)
    {
        senderInstanceId = ((chunkPos - chunkSize * myInstanceId) / (nInstances * chunkSize));
    }

    shared_ptr <MemArray> makeExchangeArray(shared_ptr <MemArray>& finalizedArray, ArrayDesc const& opSchema)
    {
        shared_ptr<MemArray> output( new MemArray(getArrayDesc(), _query));
        size_t const nInstances = _query->getInstancesCount();
        InstanceID const myInstanceId = _query->getInstanceID();
        vector <shared_ptr<MemArrayAppender> > appenders(nInstances);
        for(InstanceID i=0; i<nInstances; ++i)
        {
            size_t startingChunk;
            size_t skipInterval;
            senderToChunk(myInstanceId, i, nInstances, startingChunk, skipInterval);
            appenders[i].reset(new MemArrayAppender(output, _query, startingChunk, skipInterval));
        }
        LOG4CXX_DEBUG(logger, "FOO1: ");

        InputTriplet inputReader(finalizedArray);
        Value v;
        uint64_t hash, hashMod;
        while(!inputReader.end())
        {
            inputReader.getItems(v, hash, hashMod);
            size_t destinationInstance = hash % nInstances;
            appenders[destinationInstance]->addValues(v, hash, hashMod);
            inputReader.next();
        }
        LOG4CXX_DEBUG(logger, "FOO2: ");

        for(InstanceID i=0; i<nInstances; ++i)
        {
            appenders[i]->release();
        }
        inputReader.unlink();
        appenders.clear();
        finalizedArray.reset();
        //PhysicalOperator::dumpArrayToLog(output, logger);
        output = dynamic_pointer_cast<MemArray>(redistribute(output, _query, psHashPartitioned));
        //PhysicalOperator::dumpArrayToLog(output, logger);
        vector<shared_ptr<MemArrayBuilder> > builders(nInstances);
        InputTriplet inputReader2(output);
        while(!inputReader2.end())
        {
            inputReader2.getItems(v, hash, hashMod);
            Coordinate chunkPos = ((inputReader2.getPosition()[0]) / 1000000) * 1000000;
            size_t senderInstanceId;
            chunkToSender(chunkPos, nInstances, myInstanceId, senderInstanceId);
            if (!builders[senderInstanceId])
            {
                builders[senderInstanceId].reset(new MemArrayBuilder(getArrayDesc(), _query));
            }
            builders[senderInstanceId]->addValues(v, hash, hashMod);
            inputReader2.next();
        }

        LOG4CXX_DEBUG(logger, "FOO3: ");
        inputReader2.unlink();
        output.reset();
        vector<shared_ptr<MemArray> > pieces;
        for(InstanceID i=0; i<nInstances; ++i)
        {
            if (builders[i])
            {
                //pieces.push_back(builders[i]->finalize());
                pieces.push_back(builders[i]->finalizeNoFlush());
                builders[i].reset();
            }
        }
        size_t numPieces = pieces.size();
        if (numPieces == 0)
        {
            return shared_ptr<MemArray> (new MemArray(opSchema, _query));
        }
        LOG4CXX_DEBUG(logger, "FOO4: ");
        while (numPieces > 1)
        {
            vector< shared_ptr<MemArray> > newPieces;
            shared_ptr <MemArray> left;
            for(size_t i=0; i<numPieces; ++i)
            {
                shared_ptr<MemArray> part = pieces[i];
                if(!left)
                {
                    left = part;
                    if (i == numPieces - 1)
                    {
                        newPieces.push_back(left);
                    }
                }
                else
                {
                    LOG4CXX_DEBUG(logger, "Merging buckets "<<i-1<<" and "<<i<<" into "<<newPieces.size());
                    newPieces.push_back(HashTableMerger::merge(_query, left, part));
                    left.reset();
                }
            }
            numPieces = newPieces.size();
            pieces = newPieces;
        }
        LOG4CXX_DEBUG(logger, "FOO5: ");

        shared_ptr <MemArray> result = pieces[0];
        pieces.clear();
        MemArrayBuilder finalResult(opSchema, _query, myInstanceId * 1000000, nInstances * 1000000);
        InputTriplet inputReader3(result);
        while(!inputReader3.end())
        {
            inputReader3.getItems(v, hash, hashMod);
            finalResult.addValues(v);
            inputReader3.next();
        }
        inputReader3.unlink();
        result.reset();
        return finalResult.finalize();
    }

};



} //namespace scidb
