#ifndef GROUPED_AGGREGATE_SETTINGS
#define GROUPED_AGGREGATE_SETTINGS

#include <query/Operator.h>
#include <query/AttributeComparator.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

namespace scidb
{
namespace grouped_aggregate
{

using std::string;
using std::vector;
using std::shared_ptr;
using std::dynamic_pointer_cast;
using std::ostringstream;
using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.grouped_aggregate"));

/*
 * Settings for the grouped_aggregate operator.
 */
class Settings
{
private:
    size_t _groupSize;
    size_t _numAggs;
    size_t _maxTableSize;
    bool   _maxTableSizeSet;
    size_t _spilloverChunkSize;
    bool   _spilloverChunkSizeSet;
    size_t _mergeChunkSize;
    bool   _mergeChunkSizeSet;
    size_t _outputChunkSize;
    bool   _outputChunkSizeSet;
    size_t const _numInstances;
    bool   _inputSorted;
    bool   _inputSortedSet;
    size_t _numHashBuckets;
    bool   _numHashBucketsSet;
    vector<int64_t> _groupIds;
    vector<bool>    _isGroupOnAttribute;
    vector<string> _groupNames;
    vector<TypeId> _groupTypes;
    vector<AttributeComparator> _groupComparators;
    vector<DoubleFloatOther> _groupDfo;
    vector<AggregatePtr> _aggregates;
    vector<TypeId> _inputAttributeTypes;
    vector<TypeId> _stateTypes;
    vector<TypeId> _outputAttributeTypes;
    vector<AttributeID> _inputAttributeIds;
    vector<string> _outputAttributeNames;

public:
    Settings(ArrayDesc const& inputSchema,
             vector< shared_ptr<OperatorParam> > const& operatorParameters,
             bool logical,
             shared_ptr<Query>& query):
        _groupSize              (0),
        _numAggs                (0),
        _maxTableSize           ( Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_BUFFER)),
        _maxTableSizeSet        ( false ),
        _spilloverChunkSize     ( 100000 ),
        _spilloverChunkSizeSet  ( false ),
        _mergeChunkSize         ( 100000 ),
        _mergeChunkSizeSet      ( false ),
        _outputChunkSize        ( 100000 ),
        _outputChunkSizeSet     ( false ),
        _numInstances           ( query -> getInstancesCount() ),
        _inputSorted            ( false ),
        _inputSortedSet         ( false ),
        _numHashBuckets         ( 1000037 ),
        _numHashBucketsSet      ( false )
    {
        bool autoInputSorted = true;
        for(size_t i = 0; i<operatorParameters.size(); ++i)
        {
            shared_ptr<OperatorParam> param = operatorParameters[i];
            if (param->getParamType() == PARAM_AGGREGATE_CALL)
            {
                AttributeID inputAttId;
                string outputAttName;
                AggregatePtr agg = resolveAggregate((shared_ptr <OperatorParamAggregateCall> &) param, inputSchema.getAttributes(), &inputAttId, &outputAttName);
                _aggregates.push_back(agg);
                _stateTypes.push_back(agg->getStateType().typeId());
                _outputAttributeTypes.push_back(agg->getResultType().typeId());
                if(agg->isOrderSensitive())
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_AGGREGATION_ORDER_MISMATCH) << agg->getName();
                }
                _inputAttributeIds.push_back(inputAttId);
                _outputAttributeNames.push_back(outputAttName);
                ++_numAggs;
            }
            else if(param->getParamType() == PARAM_ATTRIBUTE_REF)
            {
                autoInputSorted=false;
                shared_ptr<OperatorParamReference> ref = dynamic_pointer_cast<OperatorParamReference> (param);
                AttributeID attId = ref->getObjectNo();
                string groupName  = ref->getObjectName();
                TypeId groupType  = inputSchema.getAttributes()[attId].getType();
                _groupIds.push_back(attId);
                _groupNames.push_back(groupName);
                _groupTypes.push_back(groupType);
                _groupComparators.push_back(AttributeComparator(groupType));
                _groupDfo.push_back(getDoubleFloatOther(groupType));
                _isGroupOnAttribute.push_back(true);
                ++_groupSize;
            }
            else if(param->getParamType() == PARAM_DIMENSION_REF)
            {
                shared_ptr<OperatorParamReference> ref = dynamic_pointer_cast<OperatorParamReference> (param);
                int64_t dimNo = ref->getObjectNo();
                if(static_cast<size_t>(dimNo) == inputSchema.getDimensions().size() - 1)
                {
                    autoInputSorted = false;
                }
                string dimName  = ref->getObjectName();
                _groupIds.push_back(dimNo);
                _groupNames.push_back(dimName);
                _groupTypes.push_back(TID_INT64);
                _groupComparators.push_back(AttributeComparator(TID_INT64));
                _groupDfo.push_back(getDoubleFloatOther(TID_INT64));
                _isGroupOnAttribute.push_back(false);
                ++_groupSize;
            }
            else
            {
                string parameterString;
                if (logical)
                {
                    parameterString = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) param)->getExpression(),query, TID_STRING).getString();
                }
                else
                {
                    parameterString = ((shared_ptr<OperatorParamPhysicalExpression>&) param)->getExpression()->evaluate().getString();
                }
                parseStringParam(parameterString);
            }
        }
        if(_numAggs == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Aggregate not specified";
        }
        if(_groupSize == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "No groups specified";
        }
        AttributeID scapeGoat = 0; //TODO: better scapegoat picking logic here (i.e. pick the smallest attribute)
        for(size_t i =0; i<_inputAttributeIds.size(); ++i)
        {
            AttributeID const& inAttId = _inputAttributeIds[i];
            if(inAttId != INVALID_ATTRIBUTE_ID)
            {
                scapeGoat = inAttId;
                break;
            }
        }
        for(size_t i =0; i<_inputAttributeIds.size(); ++i)
        {
            AttributeID& inAttId = _inputAttributeIds[i];
            if (inAttId == INVALID_ATTRIBUTE_ID)
            {
                inAttId = scapeGoat;
            }
            _inputAttributeTypes.push_back(inputSchema.getAttributes()[inAttId].getType());
        }
        if(!_inputSortedSet)
        {
            _inputSorted = autoInputSorted;
        }
        LOG4CXX_DEBUG(logger, "GAG maxTableSize "<<_maxTableSize<<
                              " spillChunkSize " <<_spilloverChunkSize<<
                              " mergeChunkSize " <<_mergeChunkSize<<
                              " outputChunkSize "<<_outputChunkSize<<
                              " sorted "<<_inputSorted<<
                              " numHashBuckets "<<_numHashBuckets);
    }

private:
    bool checkSizeTParam(string const& param, string const& header, size_t& target, bool& setFlag)
    {
        string headerWithEq = header + "=";
        if(starts_with(param, headerWithEq))
        {
            if(setFlag)
            {
                ostringstream error;
                error<<"illegal attempt to set "<<header<<" multiple times";
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
            }
            string paramContent = param.substr(headerWithEq.size());
            trim(paramContent);
            try
            {
                int64_t val = lexical_cast<int64_t>(paramContent);
                if(val<=0)
                {
                    ostringstream error;
                    error<<header<<" must be positive";
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
                }
                target = val;
                setFlag = true;
                return true;
            }
            catch (bad_lexical_cast const& exn)
            {
                ostringstream error;
                error<<"could not parse "<<error.str().c_str();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
            }
        }
        return false;
    }

    bool checkBoolParam(string const& param, string const& header, bool& target, bool& setFlag)
    {
        string headerWithEq = header + "=";
        if(starts_with(param, headerWithEq))
        {
            if(setFlag)
            {
                ostringstream error;
                error<<"illegal attempt to set "<<header<<" multiple times";
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
            }
            string paramContent = param.substr(headerWithEq.size());
            trim(paramContent);
            try
            {
                target= lexical_cast<bool>(paramContent);
                setFlag = true;
                return true;
            }
            catch (bad_lexical_cast const& exn)
            {
                ostringstream error;
                error<<"could not parse "<<error.str().c_str();
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
            }
        }
        return false;
    }

    void parseStringParam(string const& param)
    {
        if(checkSizeTParam(param,   "max_table_size",      _maxTableSize,         _maxTableSizeSet      ) ) { return; }
        if(checkSizeTParam(param,   "spill_chunk_size",    _spilloverChunkSize,   _spilloverChunkSizeSet) ) { return; }
        if(checkSizeTParam(param,   "merge_chunk_size",    _mergeChunkSize,       _mergeChunkSizeSet    ) ) { return; }
        if(checkSizeTParam(param,   "output_chunk_size",   _outputChunkSize,      _outputChunkSizeSet   ) ) { return; }
        if(checkSizeTParam(param,   "num_hash_buckets",    _numHashBuckets,       _numHashBucketsSet    ) ) { return; }
        if(checkBoolParam (param,   "input_sorted",        _inputSorted,          _inputSortedSet       ) ) { return; }
        ostringstream error;
        error<<"unrecognized parameter "<<param;
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
    }

public:
    size_t getGroupSize() const
    {
        return _groupSize;
    }

    vector<int64_t> const& getGroupIds() const
    {
        return _groupIds;
    }

    bool isGroupOnAttribute(size_t const groupNo) const
    {
        return _isGroupOnAttribute[groupNo];
    }

    vector<string> const& getGroupNames() const
    {
        return _groupNames;
    }

    vector<TypeId> const& getGroupTypes() const
    {
        return _groupTypes;
    }

    vector<TypeId> const& getInputAttributeTypes() const
    {
        return _inputAttributeTypes;
    }

    vector<AttributeID> const& getInputAttributeIds() const
    {
        return _inputAttributeIds;
    }

    vector<string> const& getOutputAttributeNames() const
    {
        return _outputAttributeNames;
    }

    vector<TypeId> const& getStateTypes() const
    {
        return _stateTypes;
    }

    vector<TypeId> const& getResultTypes() const
    {
        return _outputAttributeTypes;
    }

    AggregatePtr cloneAggregate() const
    {
        return _aggregates[0]->clone();
    }

    size_t getMaxTableSize() const
    {
        return _maxTableSize * 1024 * 1024;
    }

    size_t getSpilloverChunkSize() const
    {
        return _spilloverChunkSize;
    }

    size_t getMergeChunkSize() const
    {
        return _mergeChunkSize;
    }

    size_t getOutputChunkSize() const
    {
        return _outputChunkSize;
    }

    bool inputSorted() const
    {
        return _inputSorted;
    }

    size_t getNumHashBuckets() const
    {
        return _numHashBuckets;
    }

    size_t getNumAggs() const
    {
        return _numAggs;
    }

    enum SchemaType
    {
        SPILL,
        MERGE,
        FINAL
    };

    ArrayDesc makeSchema(SchemaType const type, string const name = "") const
    {
        Attributes outputAttributes;
        size_t i =0;
        if(type != FINAL)
        {
            outputAttributes.push_back( AttributeDesc(i++, "hash",   TID_UINT64,    0, 0));
        }
        for (size_t j =0; j<_groupSize; ++j)
        {
            outputAttributes.push_back( AttributeDesc(i++, _groupNames[j],  _groupTypes[j], 0, 0));
        }
        for (size_t j =0; j<_numAggs; ++j)
        {
            outputAttributes.push_back( AttributeDesc(i++,
                                                      _outputAttributeNames[j],
                                                      type == SPILL ? _inputAttributeTypes[j] :
                                                      type == MERGE ? _stateTypes[j] :
                                                                      _outputAttributeTypes[j],
                                                      AttributeDesc::IS_NULLABLE, 0));
        }
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        if(type == MERGE)
        {
            outputDimensions.push_back(DimensionDesc("dst_instance_id", 0, _numInstances-1, 1, 0));
            outputDimensions.push_back(DimensionDesc("src_instance_id", 0, _numInstances-1, 1, 0));
        }
        else if(type == FINAL)
        {
            outputDimensions.push_back(DimensionDesc("instance_id", 0,     _numInstances-1, 1, 0));
        }
        outputDimensions.push_back(DimensionDesc("value_no",  0, CoordinateBounds::getMax(),
                                                 type == SPILL ? _spilloverChunkSize :
                                                 type == MERGE ? _mergeChunkSize :
                                                                 _outputChunkSize, 0));
        return ArrayDesc(name.size() == 0 ? "grouped_agg_state" : name, outputAttributes, outputDimensions, defaultPartitioning());
    }

    /**
     * Determine if g is a valid group for aggregation. g must be getGroupSize() large.
     * @return true if g is a valid aggregation group, false otherwise.
     */
    inline bool groupValid(std::vector<Value const*> const& g) const
    {
        for(size_t i =0; i<_groupSize; ++i)
        {
            Value const& v = *(g[i]);
            if(v.isNull() || isNan(v, _groupDfo[i]))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Compare two groups, both must have getGroupSize() values and be groupValid().
     * @return true if g1 < g2, false otherwise.
     */
    inline bool groupLess(std::vector<Value const*> const& g1, std::vector<Value const*> const& g2) const
    {
        for(size_t i =0; i<_groupSize; ++i)
        {
            Value const& v1 = *(g1[i]);
            Value const& v2 = *(g2[i]);
            if(_groupComparators[i](v1, v2))
            {
                return true;
            }
            else if( v1 == v2 )
            {
                continue;
            }
            else
            {
                return false;
            }
        }
        return false;
    }


    /**
     * Compare two groups, both must have getGroupSize() values and be groupValid(). g1 is assumed c-style allocated.
     * @return true if g1 < g2, false otherwise.
     */
    inline bool groupLess(Value const* g1, std::vector<Value const*> const& g2) const
    {
        for(size_t i =0; i<_groupSize; ++i)
        {
            Value const& v1 = g1[i];
            Value const& v2 = *(g2[i]);
            if(_groupComparators[i](v1, v2))
            {
                return true;
            }
            else if( v1 == v2 )
            {
                continue;
            }
            else
            {
                return false;
            }
        }
        return false;
    }

    /**
     * Compare two groups, both must have getGroupSize() values and be groupValid().
     * @return true if g1 is equivalent g2, false otherwise.
     */
    inline bool groupEqual(std::vector<Value const*> g1, std::vector<Value const*> const& g2) const
    {
        for(size_t i =0; i<_groupSize; ++i)
        {
            Value const& v1 = *(g1[i]);
            Value const& v2 = *(g2[i]);
            if( v1 != v2 )
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Compare two groups, both must have getGroupSize() values and be groupValid(). g1 is assumed c-style allocated.
     * @return true if g1 is equivalent g2, false otherwise.
     */
    inline bool groupEqual(Value const* g1, std::vector<Value const*> const& g2) const
    {
        for(size_t i =0; i<_groupSize; ++i)
        {
            Value const& v1 = g1[i];
            Value const& v2 = *(g2[i]);
            if( v1 != v2 )
            {
                return false;
            }
        }
        return true;
    }

    inline void aggInitState(Value* states)
    {
        for(size_t i=0; i<_numAggs; ++i)
        {
            _aggregates[i]->initializeState( states[i] );
        }
    }

    inline void aggAccumulate(Value* states, std::vector<Value const*> const& inputs)
    {
        for(size_t i=0; i<_numAggs; ++i)
        {
            _aggregates[i]->accumulateIfNeeded( states[i], *(inputs[i]));
        }
    }

    inline void aggMerge(Value* states, std::vector<Value const*> const& inStates)
    {
        for(size_t i=0; i<_numAggs; ++i)
        {
            _aggregates[i]->mergeIfNeeded( states[i], *(inStates[i]));
        }
    }

    inline void aggFinal(Value* results, Value const* inStates)
    {
        for(size_t i=0; i<_numAggs; ++i)
        {
            _aggregates[i]->finalResult( results[i], inStates[i]);
        }
    }
};

} } //namespaces

#endif //grouped_aggregate_settings

