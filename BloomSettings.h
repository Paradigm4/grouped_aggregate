/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* rjoin is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* rjoin is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* rjoin is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with rjoin.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#ifndef BLOOM_SETTINGS
#define BLOOM_SETTINGS

#include <query/Operator.h>
#include <query/AttributeComparator.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

namespace scidb
{
namespace bloom
{

using std::string;
using std::vector;
using std::shared_ptr;
using std::dynamic_pointer_cast;
using std::ostringstream;
using std::stringstream;
using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.bloom.settings"));

/**
 * Table sizing considerations:
 *
 * We'd like to see a load factor of 4 or less. A group occupies at least 32 bytes in the structure,
 * usually more - depending on how many values and states there are and also whether they are variable sized.
 * An empty bucket is an 8-byte pointer. So the ratio of group data / bucket overhead is at least 16.
 * With that in mind we just pick a few primes for the most commonly used memory limits.
 * We start with that many buckets and, at the moment, we don't bother rehashing:
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

static size_t chooseNumBuckets(size_t maxTableSize)
{
   for(size_t i =0; i<NUM_SIZES; ++i)
   {
       if(maxTableSize <= memLimits[i])
       {
           return tableSizes[i];
       }
   }
   return tableSizes[NUM_SIZES-1];
}

//For hash join purposes, the handedness refers to which array is copied into a hash table and redistributed
enum Handedness
{
    LEFT,
    RIGHT
};

/*
 * Settings for the grouped_aggregate operator.
 */
class Settings
{
public:
    enum algorithm
    {
        HASH_REPLICATE_LEFT,
        HASH_REPLICATE_RIGHT,
        MERGE_LEFT_FIRST,
        MERGE_RIGHT_FIRST
    };

private:
    ArrayDesc                     _leftSchema;
    ArrayDesc                     _rightSchema;
    size_t                        _numLeftAttrs;
    size_t                        _numLeftDims;
    size_t                        _numRightAttrs;
    size_t                        _numRightDims;
    vector<ssize_t>               _leftMapToTuple;   //maps all attributes and dimensions from left to tuple, -1 if not used
    vector<ssize_t>               _rightMapToTuple;
    size_t                        _leftTupleSize;
    size_t                        _rightTupleSize;
    size_t                        _numKeys;
    vector<AttributeComparator>   _keyComparators;   //one per key
    vector<size_t>                _leftIds;          //key indeces in the left array:  attributes start at 0, dimensions start at numAttrs
    vector<size_t>                _rightKeys;        //key indeces in the right array: attributes start at 0, dimensions start at numAttrs
    vector<bool>                  _keyNullable;      //one per key, in the output
    size_t                        _hashJoinThreshold;
    size_t                        _numHashBuckets;
    size_t                        _chunkSize;
    size_t                        _numInstances;
    algorithm                     _algorithm;
    bool                          _algorithmSet;
    bool                          _keepDimensions;
    size_t                        _readAheadLimit;
    size_t                        _varSize;

    static string paramToString(shared_ptr <OperatorParam> const& parameter, shared_ptr<Query>& query, bool logical)
    {
        if(logical)
        {
            return evaluate(((shared_ptr<OperatorParamLogicalExpression>&) parameter)->getExpression(),query, TID_STRING).getString();
        }
        return ((shared_ptr<OperatorParamPhysicalExpression>&) parameter)->getExpression()->evaluate().getString();
    }

    void setParamKeys(string trimmedContent, vector<size_t> &keys, size_t shift)
    {
        stringstream ss(trimmedContent);
        string tok;
        while(getline(ss, tok, ','))
        {
            try
            {
                uint64_t key;
                if(tok[0] == '~')
                {
                    key = lexical_cast<uint64_t>(tok.substr(1)) + shift;
                }
                else
                {
                    key = lexical_cast<uint64_t>(tok);
                }
                keys.push_back(key);
            }
            catch (bad_lexical_cast const& exn)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse keys";
            }
        }
    }

    void setParamLeftKeys(string trimmedContent)
    {
        setParamKeys(trimmedContent, _leftIds, _numLeftAttrs);
    }

    void setParamRightKeys(string trimmedContent)
    {
        setParamKeys(trimmedContent, _rightKeys, _numRightAttrs);
    }

    void setParamHashJoinThreshold(string trimmedContent)
    {
        try
        {
            int64_t res = lexical_cast<int64_t>(trimmedContent);
            if(res <= 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "max table size must be positive";
            }
            _hashJoinThreshold = res;
            _numHashBuckets = chooseNumBuckets(_hashJoinThreshold);
        }
        catch (bad_lexical_cast const& exn)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse max table size";
        }
    }

    void setParamChunkSize(string trimmedContent)
    {
        try
        {
            int64_t res = lexical_cast<int64_t>(trimmedContent);
            if(res <= 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "chunk size must be positive";
            }
            _chunkSize = res;
        }
        catch (bad_lexical_cast const& exn)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse chunk size";
        }
    }

    void setParamAlgorithm(string trimmedContent)
    {
        if(trimmedContent == "hash_replicate_left")
        {
            _algorithm = HASH_REPLICATE_LEFT;
        }
        else if (trimmedContent == "hash_replicate_right")
        {
            _algorithm = HASH_REPLICATE_RIGHT;
        }
        else if (trimmedContent == "merge_left_first")
        {
            _algorithm = MERGE_LEFT_FIRST;
        }
        else if (trimmedContent == "merge_right_first")
        {
            _algorithm = MERGE_RIGHT_FIRST;
        }
        else
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse algorithm";
        }
    }

    void setParamKeepDimensions(string trimmedContent)
    {
        if(trimmedContent == "1" || trimmedContent == "t" || trimmedContent == "T" || trimmedContent == "true" || trimmedContent == "TRUE")
        {
            _keepDimensions = true;
        }
        else if (trimmedContent == "0" || trimmedContent == "f" || trimmedContent == "F" || trimmedContent == "false" || trimmedContent == "FALSE")
        {
            _keepDimensions = false;
        }
        else
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse keep_dimensions";
        }
    }

    void setParam (string const& parameterString, bool& alreadySet, string const& header, void (Settings::* innersetter)(string) )
    {
        string paramContent = parameterString.substr(header.size());
        if (alreadySet)
        {
            string header = parameterString.substr(0, header.size()-1);
            ostringstream error;
            error<<"illegal attempt to set "<<header<<" multiple times";
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
        }
        trim(paramContent);
        (this->*innersetter)(paramContent); //TODO:.. tried for an hour with a template first. #thisIsWhyWeCantHaveNiceThings
        alreadySet = true;
    }

public:
    static size_t const MAX_PARAMETERS = 6;

    Settings(vector<ArrayDesc const*> inputSchemas,
             vector< shared_ptr<OperatorParam> > const& operatorParameters,
             bool logical,
             shared_ptr<Query>& query):
        _leftSchema(*(inputSchemas[0])),
        _rightSchema(*(inputSchemas[1])),
        _numLeftAttrs(_leftSchema.getAttributes(true).size()),
        _numLeftDims(_leftSchema.getDimensions().size()),
        _numRightAttrs(_rightSchema.getAttributes(true).size()),
        _numRightDims(_rightSchema.getDimensions().size()),
        _hashJoinThreshold(Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_BUFFER) * 1024 * 1024 ),
        _numHashBuckets(chooseNumBuckets(_hashJoinThreshold / (1024*1024))),
        _chunkSize(1000000),
        _numInstances(query->getInstancesCount()),
        _algorithm(HASH_REPLICATE_RIGHT),
        _algorithmSet(false),
        _keepDimensions(false)
    {
        string const leftKeysHeader                = "left_ids=";
        string const rightKeysHeader               = "right_ids=";
        string const hashJoinThresholdHeader       = "hash_join_threshold=";
        string const chunkSizeHeader               = "chunk_size=";
        string const algorithmHeader               = "algorithm=";
        string const keepDimensionsHeader          = "keep_dimensions=";
        bool leftKeysSet           = false;
        bool rightKeysSet          = false;
        bool hashJoinThresholdSet  = false;
        bool chunkSizeSet          = false;
        bool keepDimensionsSet     = false;
        size_t const nParams = operatorParameters.size();
        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal number of parameters passed to rjoin";
        }
        for (size_t i= 0; i<nParams; ++i)
        {
            string parameterString = paramToString(operatorParameters[i], query, logical);
            if (starts_with(parameterString, leftKeysHeader))
            {
                setParam(parameterString, leftKeysSet, leftKeysHeader, &Settings::setParamLeftKeys);
            }
            else if (starts_with(parameterString, rightKeysHeader))
            {
                setParam(parameterString, rightKeysSet, rightKeysHeader, &Settings::setParamRightKeys);
            }
            else if (starts_with(parameterString, hashJoinThresholdHeader))
            {
                setParam(parameterString, hashJoinThresholdSet, hashJoinThresholdHeader, &Settings::setParamHashJoinThreshold);
            }
            else if (starts_with(parameterString, chunkSizeHeader))
            {
                setParam(parameterString, chunkSizeSet, chunkSizeHeader, &Settings::setParamChunkSize);
            }
            else if (starts_with(parameterString, algorithmHeader))
            {
                setParam(parameterString, _algorithmSet, algorithmHeader, &Settings::setParamAlgorithm);
            }
            else if (starts_with(parameterString, keepDimensionsHeader))
            {
                setParam(parameterString, keepDimensionsSet, keepDimensionsHeader, &Settings::setParamKeepDimensions);
            }
            else
            {
                ostringstream error;
                error << "Unrecognized token '"<<parameterString<<"'";
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
            }
        }
        verifyInputs();
        mapAttributes();
        logSettings();
    }

private:
    void throwIf(bool const cond, char const* errorText)
    {
        if(cond)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << errorText;
        }
    }

    void verifyInputs()
    {
        /*throwIf(_leftIds.size() == 0,                    "no left join-on fields provided");
        throwIf(_rightKeys.size() == 0,                  "no right join-on fields provided");
        throwIf(_leftIds.size() != _rightKeys.size(),    "mismatched numbers of keys provided");
        for(size_t i =0; i<_leftIds.size(); ++i)
        {
            size_t leftKey  = _leftIds[i];
            size_t rightKey = _rightKeys[i];
            throwIf(leftKey  >= _numLeftAttrs + _numLeftDims,  "left id out of bounds");
            throwIf(rightKey >= _numRightAttrs + _numRightDims, "right id out of bounds");
            TypeId leftType   = leftKey  < _numLeftAttrs  ? _leftSchema.getAttributes(true)[leftKey].getType()   : TID_INT64;
            TypeId rightType  = rightKey < _numRightAttrs ? _rightSchema.getAttributes(true)[rightKey].getType() : TID_INT64;
            throwIf(leftType != rightType, "key types do not match");
        }
    */
    }

    void mapAttributes()
    {
        _numKeys = _leftIds.size();
        _leftMapToTuple.resize(_numLeftAttrs + _numLeftDims, -1);
        _rightMapToTuple.resize(_numRightAttrs + _numRightDims, -1);
        for(size_t i =0; i<_numKeys; ++i)
        {
            size_t leftKey  = _leftIds[i];
            size_t rightKey = _rightKeys[i];
            throwIf(_leftMapToTuple[leftKey] != -1, "left keys not unique");
            throwIf(_rightMapToTuple[rightKey] != -1, "right keys not unique");
            _leftMapToTuple[leftKey]   = i;
            _rightMapToTuple[rightKey] = i;
            TypeId leftType   = leftKey  < _numLeftAttrs  ? _leftSchema.getAttributes(true)[leftKey].getType()   : TID_INT64;
            bool leftNullable  = leftKey  < _numLeftAttrs  ?  _leftSchema.getAttributes(true)[leftKey].isNullable()   : false;
            bool rightNullable = rightKey < _numRightAttrs  ? _rightSchema.getAttributes(true)[rightKey].isNullable() : false;
            _keyComparators.push_back(AttributeComparator(leftType));
            _keyNullable.push_back( leftNullable || rightNullable );
        }
        size_t j=_numKeys;
        for(size_t i =0; i<_numLeftAttrs + _numLeftDims; ++i)
        {
            if(_leftMapToTuple[i] == -1 && (i<_numLeftAttrs || _keepDimensions))
            {
                _leftMapToTuple[i] = j++;
            }
        }
        _leftTupleSize = j;
        j = _numKeys;
        for(size_t i =0; i<_numRightAttrs + _numRightDims; ++i)
        {
            if(_rightMapToTuple[i] == -1 && (i<_numRightAttrs || _keepDimensions))
            {
                _rightMapToTuple[i] = j++;
            }
        }
        _rightTupleSize = j;
    }

    void logSettings()
    {
        ostringstream output;
        for(size_t i=0; i<_numKeys; ++i)
        {
            output<<_leftIds[i]<<"->"<<_rightKeys[i]<<" ";
        }
        output<<"buckets "<< _numHashBuckets;
        output<<" chunk "<<_chunkSize;
        output<<" keep_dimensions "<<_keepDimensions;
        LOG4CXX_DEBUG(logger, "RJN keys "<<output.str().c_str());
    }


public:
    size_t getNumKeys() const
    {
        return _numKeys;
    }

    size_t getNumLeftAttrs() const
    {
        return _numLeftAttrs;
    }

    size_t getNumLeftDims() const
    {
        return _numLeftDims;
    }

    size_t getNumRightAttrs() const
    {
        return _numRightAttrs;
    }

    size_t getNumRightDims() const
    {
        return _numRightDims;
    }

    size_t getNumOutputAttrs() const
    {
        return _leftTupleSize + _rightTupleSize - _numKeys;
    }

    size_t getLeftTupleSize() const
    {
        return _leftTupleSize;
    }

    size_t getRightTupleSize() const
    {
        return _rightTupleSize;
    }

    size_t getNumHashBuckets() const
    {
        return _numHashBuckets;
    }

    size_t getChunkSize() const
    {
        return _chunkSize;
    }

    bool isLeftKey(size_t const i) const
    {
        if(_leftMapToTuple[i] < 0)
        {
            return false;
        }
        return static_cast<size_t>(_leftMapToTuple[i]) < _numKeys;
    }

    bool isRightKey(size_t const i) const
    {
        if(_rightMapToTuple[i] < 0)
        {
            return false;
        }
        return static_cast<size_t>(_rightMapToTuple[i]) < _numKeys;
    }

    ssize_t mapLeftToTuple(size_t const leftField) const
    {
        return _leftMapToTuple[leftField];
    }

    ssize_t mapRightToTuple(size_t const rightField) const
    {
        return _rightMapToTuple[rightField];
    }

    ssize_t mapLeftToOutput(size_t const leftField) const
    {
        return _leftMapToTuple[leftField];
    }

    ssize_t mapRightToOutput(size_t const rightField) const
    {
        if(_rightMapToTuple[rightField] == -1)
        {
            return -1;
        }
        return  isRightKey(rightField) ? _rightMapToTuple[rightField] : _rightMapToTuple[rightField] + _leftTupleSize - _numKeys;
    }

    bool keepDimensions() const
    {
        return _keepDimensions;
    }
    enum SchemaType
     {
         SPILL,
         MERGE,
         FINAL
     };

     ArrayDesc makeSchema(shared_ptr< Query> query, SchemaType const type, string const name = "") const
     {
         Attributes outputAttributes;
         size_t i =0;
             outputAttributes.push_back( AttributeDesc(i++, "hash",   TID_UINT32,    0, 0));
         outputAttributes = addEmptyTagAttribute(outputAttributes);
         Dimensions outputDimensions;
         outputDimensions.push_back(DimensionDesc("value_no",  0, CoordinateBounds::getMax(),
                                                                  100000, 0));

         return ArrayDesc(name.size() == 0 ? "bloomout" : name, outputAttributes, outputDimensions, defaultPartitioning(), query->getDefaultArrayResidency());
     }
    ArrayDesc getOutputSchema(shared_ptr< Query> query, string const name = "") const
    {
        Attributes outputAttributes(getNumOutputAttrs());
        for(AttributeID i =0; i<_numLeftAttrs; ++i)
        {
            AttributeDesc const& input = _leftSchema.getAttributes(true)[i];
            AttributeID destinationId = mapLeftToOutput(i);
            uint16_t flags = input.getFlags();
            if(isLeftKey(i) && _keyNullable[destinationId] )
            {
                flags |= AttributeDesc::IS_NULLABLE;
            }
            outputAttributes[destinationId] = AttributeDesc(destinationId, input.getName(), input.getType(), flags, 0);
        }
        for(size_t i =0; i<_numLeftDims; ++i)
        {
            ssize_t destinationId = mapLeftToOutput(i + _numLeftAttrs);
            if(destinationId < 0)
            {
                continue;
            }
            DimensionDesc const& inputDim = _leftSchema.getDimensions()[i];
            uint16_t flags = 0;
            if(isLeftKey(i + _numLeftAttrs) && _keyNullable[destinationId]) //is it joined with a nullable attribute?
            {
                flags = AttributeDesc::IS_NULLABLE;
            }
            outputAttributes[destinationId] = AttributeDesc(destinationId, inputDim.getBaseName(), TID_INT64, flags, 0);
        }
        for(AttributeID i =0; i<_numRightAttrs; ++i)
        {
            if(isRightKey(i))
            {
                continue;
            }
            AttributeDesc const& input = _rightSchema.getAttributes(true)[i];
            AttributeID destinationId = mapRightToOutput(i);
            outputAttributes[destinationId] = AttributeDesc(destinationId, input.getName(), input.getType(), input.getFlags(), 0);
        }
        for(size_t i =0; i<_numRightDims; ++i)
        {
            ssize_t destinationId = mapRightToOutput(i + _numRightAttrs);
            if(destinationId < 0 || isRightKey(i + _numRightAttrs))
            {
                continue;
            }
            DimensionDesc const& inputDim = _rightSchema.getDimensions()[i];
            outputAttributes[destinationId] = AttributeDesc(destinationId, inputDim.getBaseName(), TID_INT64, 0, 0);
        }
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("instance_id", 0, _numInstances-1,            1,          0));
        outputDimensions.push_back(DimensionDesc("value_no",    0, CoordinateBounds::getMax(), _chunkSize, 0));
        return ArrayDesc(name.size() == 0 ? "rjoin" : name, outputAttributes, outputDimensions, defaultPartitioning(), query->getDefaultArrayResidency());
    }

    template <Handedness which>
    ArrayDesc getPreSgSchema(shared_ptr< Query> query) const
    {
        size_t const numAttrs = ( which == LEFT ? _leftTupleSize : _rightTupleSize) + 1; //plus hash
        Attributes outputAttributes(numAttrs);
        outputAttributes[numAttrs-1] = AttributeDesc(numAttrs-1, "hash", TID_UINT32, 0,0);
        ArrayDesc const& inputSchema = ( which == LEFT ? _leftSchema : _rightSchema);
        for(AttributeID i = 0; i < (which == LEFT ? _numLeftAttrs : _numRightAttrs); ++i)
        {
            AttributeDesc const& input = inputSchema.getAttributes(true)[i];
            AttributeID destinationId = (which == LEFT ? mapLeftToTuple(i) : mapRightToTuple(i));
            uint16_t flags = input.getFlags();
            if( (which == LEFT ? isLeftKey(i) : isRightKey(i)) && _keyNullable[destinationId] )
            {
                flags |= AttributeDesc::IS_NULLABLE;
            }
            outputAttributes[destinationId] = AttributeDesc(destinationId, input.getName(), input.getType(), flags, 0);
        }
        for(size_t i = 0; i< (which == LEFT ? _numLeftDims : _numRightDims); ++i )
        {
            ssize_t destinationId = (which == LEFT ? mapLeftToTuple(i + _numLeftAttrs) : mapRightToTuple(i + _numRightAttrs));
            if(destinationId < 0)
            {
                continue;
            }
            DimensionDesc const& inputDim = inputSchema.getDimensions()[i];
            outputAttributes[destinationId] = AttributeDesc(destinationId, inputDim.getBaseName(), TID_INT64, 0, 0);
        }
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("dst_instance_id", 0, _numInstances-1,             1,         0));
        outputDimensions.push_back(DimensionDesc("src_instance_id", 0, _numInstances-1,             1,         0));
        outputDimensions.push_back(DimensionDesc("value_no",        0, CoordinateBounds::getMax(), _chunkSize, 0));
        return ArrayDesc("rjoin_state" , outputAttributes, outputDimensions, defaultPartitioning(), query->getDefaultArrayResidency());
    }

    vector <AttributeComparator> const& getKeyComparators() const
    {
        return _keyComparators;
    }

    algorithm getAlgorithm() const
    {
        return _algorithm;
    }

    bool algorithmSet() const
    {
        return _algorithmSet;
    }

    size_t getHashJoinThreshold() const
    {
        return _hashJoinThreshold;
    }
};

} } //namespaces

#endif //RJOIN_SETTING
