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

const std::size_t bitsPerChar = 0x08;    // 8 bits
static const unsigned char bitMask[bitsPerChar] = {
                                                       0x01,  //00000001
                                                       0x02,  //00000010
                                                       0x04,  //00000100
                                                       0x08,  //00001000
                                                       0x10,  //00010000
                                                       0x20,  //00100000
                                                       0x40,  //01000000
                                                       0x80   //10000000
                                                     };

class OptimalBloom
{
private:
    size_t _randomSeed;
	//bloom user access parameters to compute optimality
    size_t _minBloomSize;
    size_t _maxBloomSize;
    size_t _minNumHashes;
    size_t _maxNumHashes;
    size_t _estimatedElementCount;
    double _falsePosProb;
    //bloom optimal parameters
    double _pRealized;
public:

   OptimalBloom():
     _minBloomSize(1),
     _maxBloomSize(std::numeric_limits<unsigned long long int>::max()),
     _minNumHashes(1),
     _maxNumHashes(std::numeric_limits<unsigned int>::max()),
     _estimatedElementCount(10000),
     _falsePosProb(1.0 / _estimatedElementCount),
     _randomSeed(0xA5A5A5A55A5A5A5AULL)
   {}

   virtual ~OptimalBloom()
   {}

struct optimalbloom_t
{
optimalbloom_t()
:numHashes(0),
 tableSize(0)
{}
    size_t numHashes;
    size_t tableSize;
};

void throwIf(bool const cond, char const* errorText)
 {
     if(cond)
     {
         throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << errorText;
     }
 }

 void verifyBloomParams()
 {
     //throwIf(_numHashes == 0,                    "Number of Bloom hashes == 0");
 }
optimalbloom_t _optbloom;

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
  optimalbloom_t& op = _optbloom;
  op.numHashes = static_cast<unsigned int>(mink);
  op.tableSize = static_cast<unsigned long long int>(minm);
  op.tableSize += (((op.tableSize % bitsPerChar) != 0) ? (bitsPerChar - (op.tableSize % bitsPerChar)) : 0);

  if (op.numHashes < _minNumHashes)
     op.numHashes = _minNumHashes;
  else if (op.numHashes > _maxNumHashes)
     op.numHashes = _maxNumHashes;

  if (op.tableSize < _minBloomSize)
     op.tableSize = _minBloomSize;
  else if (op.tableSize > _maxBloomSize)
     op.tableSize = _maxBloomSize;

  double m = op.tableSize;
  double n = _estimatedElementCount;
  double exparg = -(m/n*std::log(2)*n/m);
  _pRealized =std::pow(1-std::exp(exparg),(m/n)*std::log(2));

  verifyBloomParams();
  LOG4CXX_DEBUG(logger,"BLOOM OPT _estimatedElementCount=" << _estimatedElementCount << " ,numHashes=" << op.numHashes << "tableSize=" << op.tableSize << ", pRealized=" << _pRealized << " ,falsePosProb=" << _falsePosProb);
  return true;
 }

	size_t getEstimatedElementCount() const
	{
		return _estimatedElementCount;
	}

	void setEstimatedElementCount(size_t estimatedElementCount)
	{
		_estimatedElementCount = estimatedElementCount;
	}

	double getFalsePosProb() const
	{
		return _falsePosProb;
	}

	void setFalsePosProb(double falsePosProb)
	{
		_falsePosProb = falsePosProb;
	}

	size_t getMaxBloomSize() const
	{
		return _maxBloomSize;
	}

	void setMaxBloomSize(size_t maxBloomSize)
	{
		_maxBloomSize = maxBloomSize;
	}

	size_t getMaxNumHashes() const
	{
		return _maxNumHashes;
	}

	void setMaxNumHashes(size_t maxNumHashes)
	{
		_maxNumHashes = maxNumHashes;
	}

	size_t getMinBloomSize() const
	{
		return _minBloomSize;
	}

	void setMinBloomSize(size_t minBloomSize)
	{
		_minBloomSize = minBloomSize;
	}

	size_t getMinNumHashes() const
	{
		return _minNumHashes;
	}

	void setMinNumHashes(size_t minNumHashes)
	{
		_minNumHashes = minNumHashes;
	}

	double getPRealized() const
	{
		return _pRealized;
	}

	void setRealized(double realized)
	{
		_pRealized = realized;
	}

	size_t getRandomSeed() const
	{
		return _randomSeed;
	}

	void setRandomSeed(size_t randomSeed)
	{
		_randomSeed = randomSeed;
	}

};

class BloomFilter
{
protected:
	typedef unsigned int bloomtype;
	typedef unsigned char celltype;
private:
    //Alex's internals
	//static uint32_t const hashSeed1 = 0x5C1DB123;
    //static uint32_t const hashSeed2 = 0xACEDBEEF;
    static size_t const defaultSize = 33554467; //about 4MB, why not?
    //BitVector _vec;  //TODO: Remove this eventually, moving to vector of unsigned ints to handle multiple hashes.
    mutable vector<char> _hashBuf;

    //bloom internals
    vector<bloomtype> _bloomvec;
    unsigned char* _bitTable;
    size_t _tableSizeBits;
    size_t _sampleCount;
    size_t _numHashes;
    unsigned long long int _randomSeed;
    size_t _insertElementCount;

    //bloom user access parameters to compute optimality
    size_t _estimatedElementCount;
    double _falsePosProb;

    size_t _minBloomSize;
    size_t _maxBloomSize;
    size_t _minNumHashes;
    size_t _maxNumHashes;

    //bloom optimal parameters
    double _pRealized;


public:
    BloomFilter(size_t const size = defaultSize):
    	_bitTable(0),
        _tableSizeBits(0),
		_numHashes(0),//arbitrary
	    _sampleCount(0),
	    _hashBuf(64),
		//below here starts the bloom optimal statistics
	    _estimatedElementCount(0),
		_insertElementCount(0),
		 _falsePosProb(0),
		_randomSeed(0xA5A5A5A55A5A5A5AULL)
    {
        generateSamples();

    }
    BloomFilter(OptimalBloom o):
    	_bitTable(0),
		_tableSizeBits(o._optbloom.tableSize),
		_numHashes(o._optbloom.numHashes),
		_sampleCount(o._optbloom.numHashes),
		_hashBuf(64),
		//below here starts the bloom optimal statistics
		_estimatedElementCount(o.getEstimatedElementCount()),
		_insertElementCount(0),
		_falsePosProb(o.getFalsePosProb()),
		_randomSeed(0xA5A5A5A55A5A5A5AULL) //TODO: might need to add something to optimal class later
    {
    	generateSamples();
    	size_t rawtablesize = _tableSizeBits / bitsPerChar;
    	try{
    		_bitTable = new celltype[static_cast<std::size_t>(rawtablesize)];
    		std::fill_n(_bitTable, rawtablesize ,0x00);
    	}catch(int e)
    	{
    		LOG4CXX_DEBUG(logger,"BLOOM OPT rawtablesize=" << rawtablesize );
    		throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Allocation failed in Bloom";
    	}
    	//_bloomvec
    }

    virtual ~BloomFilter()
      {
         delete[] _bitTable;
      }

    void generateSamples()
    {
       /*
         Note:
         A distinct hash function need not be implementation-wise
         distinct. In the current implementation "seeding" a common
         hash function with different values seems to be adequate.
       */
       const unsigned int predefSampleCount = 128;
       static const bloomtype predefSamples[predefSampleCount] =
                                  {
                                     0xAAAAAAAA, 0x55555555, 0x33333333, 0xCCCCCCCC,
                                     0x66666666, 0x99999999, 0xB5B5B5B5, 0x4B4B4B4B,
                                     0xAA55AA55, 0x55335533, 0x33CC33CC, 0xCC66CC66,
                                     0x66996699, 0x99B599B5, 0xB54BB54B, 0x4BAA4BAA,
                                     0xAA33AA33, 0x55CC55CC, 0x33663366, 0xCC99CC99,
                                     0x66B566B5, 0x994B994B, 0xB5AAB5AA, 0xAAAAAA33,
                                     0x555555CC, 0x33333366, 0xCCCCCC99, 0x666666B5,
                                     0x9999994B, 0xB5B5B5AA, 0xFFFFFFFF, 0xFFFF0000,
                                     0xB823D5EB, 0xC1191CDF, 0xF623AEB3, 0xDB58499F,
                                     0xC8D42E70, 0xB173F616, 0xA91A5967, 0xDA427D63,
                                     0xB1E8A2EA, 0xF6C0D155, 0x4909FEA3, 0xA68CC6A7,
                                     0xC395E782, 0xA26057EB, 0x0CD5DA28, 0x467C5492,
                                     0xF15E6982, 0x61C6FAD3, 0x9615E352, 0x6E9E355A,
                                     0x689B563E, 0x0C9831A8, 0x6753C18B, 0xA622689B,
                                     0x8CA63C47, 0x42CC2884, 0x8E89919B, 0x6EDBD7D3,
                                     0x15B6796C, 0x1D6FDFE4, 0x63FF9092, 0xE7401432,
                                     0xEFFE9412, 0xAEAEDF79, 0x9F245A31, 0x83C136FC,
                                     0xC3DA4A8C, 0xA5112C8C, 0x5271F491, 0x9A948DAB,
                                     0xCEE59A8D, 0xB5F525AB, 0x59D13217, 0x24E7C331,
                                     0x697C2103, 0x84B0A460, 0x86156DA9, 0xAEF2AC68,
                                     0x23243DA5, 0x3F649643, 0x5FA495A8, 0x67710DF8,
                                     0x9A6C499E, 0xDCFB0227, 0x46A43433, 0x1832B07A,
                                     0xC46AFF3C, 0xB9C8FFF0, 0xC9500467, 0x34431BDF,
                                     0xB652432B, 0xE367F12B, 0x427F4C1B, 0x224C006E,
                                     0x2E7E5A89, 0x96F99AA5, 0x0BEB452A, 0x2FD87C39,
                                     0x74B2E1FB, 0x222EFD24, 0xF357F60C, 0x440FCB1E,
                                     0x8BBE030F, 0x6704DC29, 0x1144D12F, 0x948B1355,
                                     0x6D8FD7E9, 0x1C11A014, 0xADD1592F, 0xFB3C712E,
                                     0xFC77642F, 0xF9C4CE8C, 0x31312FB9, 0x08B0DD79,
                                     0x318FA6E7, 0xC040D23D, 0xC0589AA7, 0x0CA5C075,
                                     0xF874B172, 0x0CF914D5, 0x784D3280, 0x4E8CFEBC,
                                     0xC569F575, 0xCDB2A091, 0x2CC016B4, 0x5C5F4421
                                  };

       if (_sampleCount <= predefSampleCount)
       {
          std::copy(predefSamples,
                    predefSamples + _sampleCount,
                    std::back_inserter(_bloomvec));
           for (unsigned int i = 0; i < _bloomvec.size(); ++i)
           {
             /*
               Note:
               This is done to integrate the user defined random seed,
               so as to allow for the generation of unique bloom filter
               instances.
             */
        	 //TODO: Review the below line of code.
             _bloomvec[i] = _bloomvec[i] * _bloomvec[(i + 3) % _bloomvec.size()] + static_cast<bloomtype>(_randomSeed);
           }
       }
       else
       {
          std::copy(predefSamples,predefSamples + predefSampleCount,std::back_inserter(_bloomvec));
          srand(static_cast<unsigned int>(_randomSeed));
          while (_bloomvec.size() < _sampleCount)
          {
             bloomtype currentsample = static_cast<bloomtype>(rand()) * static_cast<bloomtype>(rand());
             if (0 == currentsample) continue;
             if (_bloomvec.end() == std::find(_bloomvec.begin(), _bloomvec.end(), currentsample))
             {
                _bloomvec.push_back(currentsample);
             }
          }
       }
    }

    inline virtual void computeIndices(const bloomtype& hash, std::size_t& bitindex, std::size_t& bit) const
    {
       bitindex = hash % _tableSizeBits;
       bit = bitindex % bitsPerChar;
    }

    inline unsigned int hash_ap(const unsigned char* begin, std::size_t remaining_length, unsigned int hash) const
    {
       const unsigned char* itr = begin;
       unsigned int loop = 0;
       while (remaining_length >= 8)
       {
          const unsigned int& i1 = *(reinterpret_cast<const unsigned int*>(itr)); itr += sizeof(unsigned int);
          const unsigned int& i2 = *(reinterpret_cast<const unsigned int*>(itr)); itr += sizeof(unsigned int);
          hash ^= (hash <<  7) ^  i1 * (hash >> 3) ^
               (~((hash << 11) + (i2 ^ (hash >> 5))));
          remaining_length -= 8;
       }
       if (remaining_length)
       {
          if (remaining_length >= 4)
          {
             const unsigned int& i = *(reinterpret_cast<const unsigned int*>(itr));
             if (loop & 0x01)
                hash ^=    (hash <<  7) ^  i * (hash >> 3);
             else
                hash ^= (~((hash << 11) + (i ^ (hash >> 5))));
             ++loop;
             remaining_length -= 4;
             itr += sizeof(unsigned int);
          }
          if (remaining_length >= 2)
          {
             const unsigned short& i = *(reinterpret_cast<const unsigned short*>(itr));
             if (loop & 0x01)
                hash ^=    (hash <<  7) ^  i * (hash >> 3);
             else
                hash ^= (~((hash << 11) + (i ^ (hash >> 5))));
             ++loop;
             remaining_length -= 2;
             itr += sizeof(unsigned short);
          }
          if (remaining_length)
          {
             hash += ((*itr) ^ (hash * 0xA5A5A5A5)) + loop;
          }
       }
       return hash;
    }


    inline virtual void addData(const unsigned char* keybegin, const std::size_t& length)
      {
         std::size_t bitindex = 0;
         std::size_t bit = 0;
         for (std::size_t i = 0; i < _bloomvec.size(); ++i)
         {
        	 //TODO:Shoud utilize murmur hash here instad of hash_ap
        	 //LOG4CXX_DEBUG(logger,"BLOOM addData bloomvec.size()=" << _bloomvec.size() );
        	 computeIndices(hash_ap(keybegin,length,_bloomvec[i]),bitindex,bit);
        	 //LOG4CXX_DEBUG(logger,"BLOOM addData bitindex=" << bitindex << ", bit=" << bit );
        	 _bitTable[bitindex / bitsPerChar] |= bitMask[bit];
         }
         ++_insertElementCount;
      }

    template<typename T>
    inline void addData(const T& t)
    {
       // Note: T must be a C++ POD type.
       addData(reinterpret_cast<const unsigned char*>(&t),sizeof(T));
    }

    inline void addData(const std::string& key)
    {
       addData(reinterpret_cast<const unsigned char*>(key.c_str()),key.size());
    }

    inline void addData(const char* data, const std::size_t& length)
     {
        addData(reinterpret_cast<const unsigned char*>(data),length);
     }

    template<typename InputIterator>
    inline void insert(const InputIterator begin, const InputIterator end)
    {
       InputIterator itr = begin;
       while (end != itr)
       {
          addData(*(itr++));
       }
    }

    inline virtual bool hasData(const unsigned char* key_begin, const std::size_t length) const
    {
       std::size_t bitindex = 0;
       std::size_t bit = 0;
       for (std::size_t i = 0; i < _bloomvec.size(); ++i)
       {
          computeIndices(hash_ap(key_begin,length,_bloomvec[i]),bitindex,bit);
          if ((_bitTable[bitindex / bitsPerChar] & bitMask[bit]) != bitMask[bit])
          {
             return false;
          }
       }
       return true;
    }

    template<typename T>
    inline bool hasData(const T& t) const
    {
    	return hasData(reinterpret_cast<const unsigned char*>(&t),static_cast<std::size_t>(sizeof(T)));
    }

    inline bool hasData(const std::string& key) const
    {
    	return hasData(reinterpret_cast<const unsigned char*>(key.c_str()),key.size());
    }

    inline bool hasData(const char* data, const std::size_t& length) const
    {
    	return hasData(reinterpret_cast<const unsigned char*>(data),length);
    }
    template<typename InputIterator>
    inline InputIterator containsall(const InputIterator begin, const InputIterator end) const
    {
    	InputIterator itr = begin;
    	while (end != itr)
    	{
    		if (!contains(*itr))
    		{
    			return itr;
    		}
    		++itr;
    	}
    	return end;
    }
    /* bool hasData(char const* data, size_t const dataSize ) const
    {
        uint32_t hash1 = JoinHashTable::murmur3_32(data, dataSize, hashSeed1) % _vec.getBitSize();
        uint32_t hash2 = JoinHashTable::murmur3_32(data, dataSize, hashSeed2) % _vec.getBitSize();
        return _vec.get(hash1) && _vec.get(hash2);
    }
    */
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

    /*void throwIf(bool const cond, char const* errorText)
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
    */

     /*
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
*/
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


};

} } //namespace scidb::bloom

#endif
