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

#ifndef BLOOM_UTILITIES
#define _UTILITIES

#include <query/Operator.h>
#include <query/AttributeComparator.h>
#include <util/arena/Set.h>
#include <util/arena/Map.h>
#include <util/arena/Vector.h>
#include <util/arena/List.h>
#include <util/Arena.h>
#include <array/SortArray.h>
#include <array/TupleArray.h>
#include "BloomSettings.h"
#include "JoinHashTable.h"

namespace scidb
{
namespace bloom
{

using scidb::arena::Options;
using scidb::arena::ArenaPtr;
using scidb::arena::newArena;
using scidb::SortArray;
using std::shared_ptr;
using std::dynamic_pointer_cast;
using bloom::Settings;
class BitVector
{
private:
    size_t const _size;
    vector<char> _data;

public:
    BitVector (size_t const bitSize):
        _size(bitSize),
        _data( (_size+7) / 8, 0)
    {}

    BitVector(size_t const bitSize, void const* data):
        _size(bitSize),
        _data( (_size+7) / 8, 0)
    {
        memcpy(&(_data[0]), data, _data.size());
    }

    void set(size_t const& idx)
    {
        if(idx >= _size)
        {
            throw 0;
        }
        size_t byteIdx = idx / 8;
        size_t bitIdx  = idx - byteIdx * 8;
        char& b = _data[ byteIdx ];
        b = b | (1 << bitIdx);
    }

    bool get(size_t const& idx) const
    {
        if(idx >= _size)
        {
            throw 0;
        }
        size_t byteIdx = idx / 8;
        size_t bitIdx  = idx - byteIdx * 8;
        char const& b = _data[ byteIdx ];
        return (b & (1 << bitIdx));
    }

    size_t getBitSize() const
    {
        return _size;
    }

    size_t getByteSize() const
    {
        return _data.size();
    }

    char const* getData() const
    {
        return &(_data[0]);
    }

    void orIn(BitVector const& other)
    {
        if(other._size != _size)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "OR-ing in unequal vit vectors";
        }
        for(size_t i =0; i<_data.size(); ++i)
        {
            _data[i] != other._data[i];
        }
    }
};

/*Take a look at the optimal number of hash functions relationship:
          k: number of hash functions used in the bloom filter ( cant have too many or too few )
          m: number of buckets in bloom filter ( more buckets = more ram usage )
          n: number of elements
          k = m / n * ln2

          In my case, I dont always know n exactly.
          I'd like m to be palatable .. ~~ 8MB maybe. But this I can check and adjust...
          So what should my k be?
*/
class BloomFilter
{

private:
    static uint32_t const hashSeed1 = 0x5C1DB123;
    static uint32_t const hashSeed2 = 0xACEDBEEF;
    static size_t const defaultSize = 33554467; //about 4MB, why not?
    const std::size_t bitsPerChar = 0x08;    // 8 bits

    BitVector _vec;
    mutable vector<char> _hashBuf;

    //bloom parameters to compute optimality
    size_t _minBloomSize;
    size_t _maxBloomSize;
    size_t _minNumHashes;
    size_t _maxNumHashes;
    size_t _estimatedElementCount;
    double _falsePosProb;
    //bloom optimal parameters
    bool _optimalSet;
    size_t _numHashes;
    size_t _tableSize;
    double _pRealized;


public:
    BloomFilter(size_t const size = defaultSize):
        _vec(size),
        _hashBuf(64),
		  //below here starts the bloom optimal statistics
		  _minBloomSize(1),
		  _maxBloomSize(std::numeric_limits<size_t>::max()),
		  _minNumHashes(1),
		  _maxNumHashes(std::numeric_limits<size_t>::max()),
		  _estimatedElementCount(100000),
		  _falsePosProb(1.0 / _estimatedElementCount),
		  _optimalSet(false)
    {}

    void addData( char const* data, size_t const dataSize )
    {
        uint32_t hash1 = JoinHashTable::murmur3_32(data, dataSize, hashSeed1) % _vec.getBitSize();
        uint32_t hash2 = JoinHashTable::murmur3_32(data, dataSize, hashSeed2) % _vec.getBitSize();
        _vec.set(hash1);
        _vec.set(hash2);
    }

    bool hasData(char const* data, size_t const dataSize ) const
    {
        uint32_t hash1 = JoinHashTable::murmur3_32(data, dataSize, hashSeed1) % _vec.getBitSize();
        uint32_t hash2 = JoinHashTable::murmur3_32(data, dataSize, hashSeed2) % _vec.getBitSize();
        return _vec.get(hash1) && _vec.get(hash2);
    }

    void addTuple(vector<Value const*> data, size_t const nKeys)
    {
        size_t totalSize = 0;
        for(size_t i=0; i<nKeys; ++i)
        {
            totalSize+=data[i]->size();
        }
        if(_hashBuf.size() < totalSize)
        {
            _hashBuf.resize(totalSize);
        }
        char* ch = &_hashBuf[0];
        for(size_t i =0; i<nKeys; ++i)
        {
            memcpy(ch, data[i]->data(), data[i]->size());
            ch += data[i]->size();
        }
        addData(&_hashBuf[0], totalSize);
    }

    bool hasTuple(vector<Value const*> data, size_t const nKeys) const
    {
        size_t totalSize = 0;
        for(size_t i=0; i<nKeys; ++i)
        {
            totalSize+=data[i]->size();
        }
        if(_hashBuf.size() < totalSize)
        {
            _hashBuf.resize(totalSize);
        }
        char* ch = &_hashBuf[0];
        for(size_t i =0; i<nKeys; ++i)
        {
            memcpy(ch, data[i]->data(), data[i]->size());
            ch += data[i]->size();
        }
        return hasData(&_hashBuf[0], totalSize);
    }

    void setEstimatedElementCount(size_t set)
    {
    	_estimatedElementCount = set;
    }
    void setMinBloomSize(size_t set)
    {
    	_minBloomSize = set;
    }
    void setMaxBloomSize(size_t set)
    {
        _maxBloomSize = set;
    }
    void setMinNumHashes(size_t set)
    {
        _minNumHashes = set;
    }
    void setMaxNumHashes(size_t set)
    {
        _maxNumHashes = set;
    }

    void throwIf(bool const cond, char const* errorText)
     {
         if(cond)
         {
             throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << errorText;
         }
     }

     void verifyBloomParams()
     {
         throwIf(_numHashes == 0,                    "Number of Bloom hashes == 0");
     }

     virtual bool applyOptimalParameters()
     {
    	 if(_optimalSet)
    	 {
    		 //_numHashes = ;
    		 //_tableSize = ;
			 return true;
    	 }
    	 else
    		 return false;
     }

     virtual bool computeOptimalParameters()
    {
    	/*we have an estimated count, know the approx maximum amount of memory that we can use, and have an idea
    	 *as to the value of falsePos probability. Attempt to find the number of hash functions
         *and minimum amount of storage
    	*/
       double maxnumhashes = 1000;
       double minm = std::numeric_limits<double>::infinity(); // I believe this is the table size.
       double mink = 0.0;
       double currm = 0.0;
       double k = 1.0;

       while (k < maxnumhashes)
       {
          double num   = (- k * _estimatedElementCount);
          double denom = std::log(1.0 - std::pow(_falsePosProb, 1.0 / k));
          currm = num / denom;
          if (currm < minm)
          {
             minm = currm;
             mink = k;
          }
          k += 1.0;
       }

       _numHashes = static_cast<unsigned int>(mink);
       _tableSize = static_cast<unsigned long long int>(minm);
       _tableSize += (((_tableSize % bitsPerChar) != 0) ? (bitsPerChar - (_tableSize % bitsPerChar)) : 0);

       if (_numHashes < _minNumHashes)
          _numHashes = _minNumHashes;
       else if (_numHashes > _maxNumHashes)
          _numHashes = _maxNumHashes;

       if (_tableSize < _minBloomSize)
          _tableSize = _minBloomSize;
       else if (_tableSize > _maxBloomSize)
          _tableSize = _maxBloomSize;

       double m = _tableSize;
       double n = _estimatedElementCount;
       double exparg = -(m/n*std::log(2)*n/m);
       _pRealized =std::pow(1-std::exp(exparg),(m/n)*std::log(2));

       verifyBloomParams()
       _optimalSet = true;
       LOG4CXX_DEBUG(logger,"BLOOM OPT _estimatedElementCount=" << _estimatedElementCount << " ,numHashes=" << _numHashes << "tableSize=" << _tableSize << ", pRealized=" << _pRealized << " ,falsePosProb=" << _falsePosProb);
       return true;
    }

    /*
    void globalExchange(shared_ptr<Query>& query)
    {
       size_t const nInstances = query->getInstancesCount();
       InstanceID myId = query->getInstanceID();
       std::shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, _vec.getByteSize()));
       memcpy(buf->getData(), _vec.getData(), _vec.getByteSize());
       for(InstanceID i=0; i<nInstances; i++)
       {
           if(i != myId)
           {
               BufSend(i, buf, query);
           }
       }
       for(InstanceID i=0; i<nInstances; i++)
       {
           if(i != myId)
           {
               buf = BufReceive(i,query);
               if(buf->getSize() != _vec.getByteSize())
               {
                   throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "exchanging unequal bit vectors";
               }
               BitVector incoming(_vec.getBitSize(), buf->getData());
               _vec.orIn(incoming);
           }
       }
    }

  */

			size_t getEstimatedElementCount() const {
				return _estimatedElementCount;
			}

			double getFalsePosProb() const {
				return _falsePosProb;
			}

			void setFalsePosProb(double falsePosProb) {
				_falsePosProb = falsePosProb;
			}
};

} } //namespace scidb::bloom

#endif
