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
    vector<AttributeID> _groupAttributeIds;
    vector<string> _groupAttributeNames;
    vector<TypeId> _groupAttributeTypes;
    size_t _groupSize;
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
    TypeId _inputAttributeType;
    TypeId _stateType;
    TypeId _outputAttributeType;
    AttributeID  _inputAttributeId;
    string _outputAttributeName;
    AggregatePtr _aggregate;
    vector<AttributeComparator> _groupComparators;
    vector<DoubleFloatOther>    _groupDfo;

public:
    Settings(ArrayDesc const& inputSchema,
             vector< shared_ptr<OperatorParam> > const& operatorParameters,
             bool logical,
             shared_ptr<Query>& query):
        _groupSize              (0),
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
                if(_aggregate.get() != NULL)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Aggregate specified multiple times";
                }
                _aggregate = resolveAggregate((shared_ptr <OperatorParamAggregateCall> &) param, inputSchema.getAttributes(), &_inputAttributeId, &_outputAttributeName);
                _stateType = _aggregate->getStateType().typeId();
                _outputAttributeType = _aggregate->getResultType().typeId();
                if(_aggregate->isOrderSensitive())
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_AGGREGATION_ORDER_MISMATCH) << _aggregate->getName();
                }
                _inputAttributeType = inputSchema.getAttributes()[_inputAttributeId].getType();
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
                _groupSize++;
            }
        }
        if(_aggregate.get() == NULL)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Aggregate not specified";
        }
        if(_groupSize == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "No groups specified";
        }
        if(_inputAttributeId == INVALID_ATTRIBUTE_ID)
        {
            _inputAttributeId = _groupAttributeIds[0];
        }
    }

    vector<AttributeID> const& getGroupAttributeIds() const
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

    TypeId const& getInputAttributeType() const
    {
        return _inputAttributeType;
    }

    AttributeID getInputAttributeId() const
    {
        return _inputAttributeId;
    }

    string const& getOutputAttributeName() const
    {
        return _outputAttributeName;
    }

    TypeId const& getStateType() const
    {
        return _stateType;
    }

    TypeId const& getResultType() const
    {
        return _outputAttributeType;
    }

    AggregatePtr cloneAggregate() const
    {
        return _aggregate->clone();
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
        outputAttributes.push_back( AttributeDesc(i++,
                                                  _outputAttributeName,
                                                  type == SPILL ? _inputAttributeType :
                                                  type == MERGE ? _stateType :
                                                                  _outputAttributeType,
                                                  AttributeDesc::IS_NULLABLE, 0));
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

    size_t getGroupSize() const
    {
        return _groupSize;
    }

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
};

} } //namespaces

#endif //grouped_aggregate_settings

