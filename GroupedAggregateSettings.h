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
    TypeId _inputAttributeType;
    TypeId _stateType;
    TypeId _resultType;
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
        _groupAttributeType     (inputSchema.getAttributes()[_groupAttributeId].getType())
    {
        _aggregate = resolveAggregate((shared_ptr <OperatorParamAggregateCall> &) operatorParameters[1], inputSchema.getAttributes(), &_inputAttributeId, &_outputAttributeName);
        _stateType = _aggregate->getStateType().typeId();
        _resultType = _aggregate->getResultType().typeId();
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

    AttributeID getGroupAttributeId()
    {
        return _groupAttributeId;
    }

    string const& getGroupAttributeName()
    {
        return _groupAttributeName;
    }

    TypeId const& getGroupAttributeType()
    {
        return _groupAttributeType;
    }

    TypeId const& getInputAttributeType()
    {
        return _inputAttributeType;
    }

    AttributeID getInputAttributeId()
    {
        return _inputAttributeId;
    }

    string const& getOutputAttributeName()
    {
        return _outputAttributeName;
    }

    TypeId const& getStateType()
    {
        return _stateType;
    }

    TypeId const& getResultType()
    {
        return _resultType;
    }

    AggregatePtr cloneAggregate()
    {
        return _aggregate->clone();
    }
};

} } //namespaces

#endif //grouped_aggregate_settings

