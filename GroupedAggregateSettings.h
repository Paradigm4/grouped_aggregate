#ifndef GROUPED_AGGREGATE_SETTINGS
#define GROUPED_AGGREGATE_SETTINGS

#include <query/Operator.h>
#include <query/AttributeComparator.h>

namespace scidb
{
namespace grouped_aggregate
{

using std::string;
using std::vector;
using std::shared_ptr;
using std::dynamic_pointer_cast;


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
    vector<int64_t> _groupAttributeIds;
    vector<string> _groupAttributeNames;
    vector<TypeId> _groupAttributeTypes;
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
        _maxTableSize           ( Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_BUFFER) * 1024 * 1024 ),
        _maxTableSizeSet        ( false ),
        _spilloverChunkSize     ( 1000000 ),
        _spilloverChunkSizeSet  ( false ),
        _mergeChunkSize         ( 1000000 ),
        _mergeChunkSizeSet      ( false ),
        _outputChunkSize        ( 1000000 ),
        _outputChunkSizeSet     ( false ),
        _numInstances           ( query -> getInstancesCount() ),
        _inputSorted            ( false ),
        _inputSortedSet         ( false )
    {
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
                _inputAttributeTypes.push_back(inputSchema.getAttributes()[inputAttId].getType());
                _inputAttributeIds.push_back(inputAttId);
                _outputAttributeNames.push_back(outputAttName);
                ++_numAggs;
            }
            else if(param->getParamType() == PARAM_ATTRIBUTE_REF)
            {
                shared_ptr<OperatorParamReference> ref = dynamic_pointer_cast<OperatorParamReference> (param);
                AttributeID attId = ref->getObjectNo();
                string groupName  = ref->getObjectName();
                TypeId groupType  = inputSchema.getAttributes()[attId].getType();
                _groupAttributeIds.push_back(attId);
                _groupAttributeNames.push_back(groupName);
                _groupAttributeTypes.push_back(groupType);
                _groupComparators.push_back(AttributeComparator(groupType));
                _groupDfo.push_back(getDoubleFloatOther(groupType));
                ++_groupSize;
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
        AttributeID scapeGoat = inputSchema.getAttributes().size() - 1; //TODO: better scapegoat picking logic here
        for(size_t i =0; i<_inputAttributeIds.size(); ++i)
        {
            AttributeID& inAttId = _inputAttributeIds[i];
            if (inAttId == INVALID_ATTRIBUTE_ID)
            {
                inAttId = scapeGoat;
            }
            _inputAttributeTypes.push_back(inputSchema.getAttributes()[inAttId].getType());
        }
    }

    size_t getGroupSize() const
    {
        return _groupSize;
    }

    vector<int64_t> const& getGroupAttributeIds() const
    {
        return _groupAttributeIds;
    }

    vector<string> const& getGroupAttributeNames() const
    {
        return _groupAttributeNames;
    }

    vector<TypeId> const& getGroupAttributeTypes() const
    {
        return _groupAttributeTypes;
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
        return _maxTableSize;
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
            outputAttributes.push_back( AttributeDesc(i++, _groupAttributeNames[j],  _groupAttributeTypes[j], 0, 0));
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

