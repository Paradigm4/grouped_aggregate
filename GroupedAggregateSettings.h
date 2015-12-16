#ifndef GROUPED_AGGREGATE_SETTINGS
#define GROUPED_AGGREGATE_SETTINGS

#include <query/Operator.h>

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
    AttributeID const _groupAttributeId;
    string const _groupAttributeName;
    TypeId const _groupAttributeType;
    size_t _maxTableSize;
    bool   _maxTableSizeSet;
    size_t _spilloverChunkSize;
    bool   _spilloverChunkSizeSet;
    size_t _mergeChunkSize;
    bool   _mergeChunkSizeSet;
    size_t _outputChunkSize;
    bool   _outputChunkSizeSet;
    size_t const _numInstances;
    TypeId _inputAttributeType;
    TypeId _stateType;
    TypeId _outputAttributeType;
    AttributeID  _inputAttributeId;
    string _outputAttributeName;
    AggregatePtr _aggregate;

public:
    Settings(ArrayDesc const& inputSchema,
             vector< shared_ptr<OperatorParam> > const& operatorParameters,
             bool logical,
             shared_ptr<Query>& query):
        _groupAttributeId       (dynamic_pointer_cast<OperatorParamReference> (operatorParameters[0])->getObjectNo()),
        _groupAttributeName     (dynamic_pointer_cast<OperatorParamReference> (operatorParameters[0])->getObjectName()),
        _groupAttributeType     (inputSchema.getAttributes()[_groupAttributeId].getType()),
        _maxTableSize           ( Config::getInstance()->getOption<int>(CONFIG_MERGE_SORT_BUFFER) * 1024 * 1024 ),
        _maxTableSizeSet        ( false ),
        _spilloverChunkSize     ( 1000000 ),
        _spilloverChunkSizeSet  ( false ),
        _mergeChunkSize         ( 1000000 ),
        _mergeChunkSizeSet      ( false ),
        _outputChunkSize        ( 1000000 ),
        _outputChunkSizeSet     ( false ),
        _numInstances           ( query -> getInstancesCount() )
    {
        _aggregate = resolveAggregate((shared_ptr <OperatorParamAggregateCall> &) operatorParameters[1], inputSchema.getAttributes(), &_inputAttributeId, &_outputAttributeName);
        _stateType = _aggregate->getStateType().typeId();
        _outputAttributeType = _aggregate->getResultType().typeId();
        if(_inputAttributeId == INVALID_ATTRIBUTE_ID)
        {
            _inputAttributeId = _groupAttributeId;
        }
        if(_aggregate->isOrderSensitive())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_AGGREGATION_ORDER_MISMATCH) << _aggregate->getName();
        }
        _inputAttributeType = inputSchema.getAttributes()[_inputAttributeId].getType();
    }

    AttributeID getGroupAttributeId() const
    {
        return _groupAttributeId;
    }

    string const& getGroupAttributeName() const
    {
        return _groupAttributeName;
    }

    TypeId const& getGroupAttributeType() const
    {
        return _groupAttributeType;
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

    enum SchemaType
    {
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
        outputAttributes.push_back( AttributeDesc(i++, _groupAttributeName,  _groupAttributeType, 0, 0));
        outputAttributes.push_back( AttributeDesc(i++, _outputAttributeName, type == MERGE ? _stateType : _outputAttributeType, AttributeDesc::IS_NULLABLE, 0));
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("dst_instance_id", 0, _numInstances-1, 1, 0));
        outputDimensions.push_back(DimensionDesc("src_instance_id", 0, _numInstances-1, 1, 0));
        outputDimensions.push_back(DimensionDesc("value_no",        0, CoordinateBounds::getMax(), type == MERGE ? _mergeChunkSize : _outputChunkSize, 0));
        return ArrayDesc(name.size() == 0 ? "grouped_agg_state" : name, outputAttributes, outputDimensions, defaultPartitioning());
    }
};

} } //namespaces

#endif //grouped_aggregate_settings

