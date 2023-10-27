#include "common/exception/binder.h"
#include "common/exception/catalog.h"
#include "common/string_format.h"
#include "function/aggregate/collect.h"
#include "function/aggregate/count.h"
#include "function/aggregate/count_star.h"
#include "function/aggregate_function.h"
#include "function/arithmetic/vector_arithmetic_functions.h"
#include "function/blob/vector_blob_functions.h"
#include "function/built_in_function.h"
#include "function/cast/vector_cast_functions.h"
#include "function/comparison/vector_comparison_functions.h"
#include "function/date/vector_date_functions.h"
#include "function/interval/vector_interval_functions.h"
#include "function/list/vector_list_functions.h"
#include "function/map/vector_map_functions.h"
#include "function/path/vector_path_functions.h"
#include "function/schema/vector_node_rel_functions.h"
#include "function/string/vector_string_functions.h"
#include "function/struct/vector_struct_functions.h"
#include "function/table_functions/call_functions.h"
#include "function/timestamp/vector_timestamp_functions.h"
#include "function/union/vector_union_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

BuiltInFunctions::BuiltInFunctions() {
    registerScalarFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
}

void BuiltInFunctions::registerScalarFunctions() {
    registerComparisonFunctions();
    registerArithmeticFunctions();
    registerDateFunctions();
    registerTimestampFunctions();
    registerIntervalFunctions();
    registerStringFunctions();
    registerCastFunctions();
    registerListFunctions();
    registerStructFunctions();
    registerMapFunctions();
    registerUnionFunctions();
    registerNodeRelFunctions();
    registerPathFunctions();
    registerBlobFunctions();
}

void BuiltInFunctions::registerAggregateFunctions() {
    registerCountStar();
    registerCount();
    registerSum();
    registerAvg();
    registerMin();
    registerMax();
    registerCollect();
}

ScalarFunction* BuiltInFunctions::matchScalarFunction(
    const std::string& name, const std::vector<LogicalType>& inputTypes) {
    auto& functionDefinitions = functions.at(name);
    bool isOverload = functionDefinitions.size() > 1;
    std::vector<ScalarFunction*> candidateFunctions;
    uint32_t minCost = UINT32_MAX;
    for (auto& functionDefinition : functionDefinitions) {
        auto scalarFunc = reinterpret_cast<ScalarFunction*>(functionDefinition.get());
        auto cost = getFunctionCost(inputTypes, scalarFunc, isOverload);
        if (cost == UINT32_MAX) {
            continue;
        }
        if (cost < minCost) {
            candidateFunctions.clear();
            candidateFunctions.push_back(scalarFunc);
            minCost = cost;
        } else if (cost == minCost) {
            candidateFunctions.push_back(scalarFunc);
        }
    }
    validateNonEmptyCandidateFunctions(candidateFunctions, name, inputTypes);
    if (candidateFunctions.size() > 1) {
        return getBestMatch(candidateFunctions);
    }
    return candidateFunctions[0];
}

AggregateFunction* BuiltInFunctions::matchAggregateFunction(
    const std::string& name, const std::vector<common::LogicalType>& inputTypes, bool isDistinct) {
    auto& functionDefinitions = functions.at(name);
    std::vector<AggregateFunction*> candidateFunctions;
    for (auto& functionDefinition : functionDefinitions) {
        auto aggregateFunc = reinterpret_cast<AggregateFunction*>(functionDefinition.get());
        auto cost = getAggregateFunctionCost(inputTypes, isDistinct, aggregateFunc);
        if (cost == UINT32_MAX) {
            continue;
        }
        candidateFunctions.push_back(aggregateFunc);
    }
    validateNonEmptyCandidateFunctions(candidateFunctions, name, inputTypes, isDistinct);
    assert(candidateFunctions.size() == 1);
    return candidateFunctions[0];
}

TableFunction* BuiltInFunctions::mathTableFunction(const std::string& name) {
    auto upperName = name;
    StringUtils::toUpper(upperName);
    containsFunction(upperName);
    return reinterpret_cast<TableFunction*>(functions.at(upperName)[0].get());
}

uint32_t BuiltInFunctions::getCastCost(LogicalTypeID inputTypeID, LogicalTypeID targetTypeID) {
    if (inputTypeID == targetTypeID) {
        return 0;
    } else {
        if (targetTypeID == LogicalTypeID::ANY) {
            // Any inputTypeID can match to type ANY
            return 0;
        }
        switch (inputTypeID) {
        case LogicalTypeID::ANY:
            // ANY type can be any type
            return 0;
        case LogicalTypeID::INT64:
            return castInt64(targetTypeID);
        case LogicalTypeID::INT32:
            return castInt32(targetTypeID);
        case LogicalTypeID::INT16:
            return castInt16(targetTypeID);
        case LogicalTypeID::INT8:
            return castInt8(targetTypeID);
        case LogicalTypeID::UINT64:
            return castUInt64(targetTypeID);
        case LogicalTypeID::UINT32:
            return castUInt32(targetTypeID);
        case LogicalTypeID::UINT16:
            return castUInt16(targetTypeID);
        case LogicalTypeID::UINT8:
            return castUInt8(targetTypeID);
        case LogicalTypeID::INT128:
            return castInt128(targetTypeID);
        case LogicalTypeID::DOUBLE:
            return castDouble(targetTypeID);
        case LogicalTypeID::FLOAT:
            return castFloat(targetTypeID);
        case LogicalTypeID::DATE:
            return castDate(targetTypeID);
        case LogicalTypeID::SERIAL:
            return castSerial(targetTypeID);
        default:
            return UNDEFINED_CAST_COST;
        }
    }
}

uint32_t BuiltInFunctions::getAggregateFunctionCost(
    const std::vector<LogicalType>& inputTypes, bool isDistinct, AggregateFunction* function) {
    if (inputTypes.size() != function->parameterTypeIDs.size() ||
        isDistinct != function->isDistinct) {
        return UINT32_MAX;
    }
    for (auto i = 0u; i < inputTypes.size(); ++i) {
        if (function->parameterTypeIDs[i] == LogicalTypeID::ANY) {
            continue;
        } else if (inputTypes[i].getLogicalTypeID() != function->parameterTypeIDs[i]) {
            return UINT32_MAX;
        }
    }
    return 0;
}

void BuiltInFunctions::validateNonEmptyCandidateFunctions(
    std::vector<AggregateFunction*>& candidateFunctions, const std::string& name,
    const std::vector<LogicalType>& inputTypes, bool isDistinct) {
    if (candidateFunctions.empty()) {
        std::string supportedInputsString;
        for (auto& functionDefinition : functions.at(name)) {
            auto aggregateFunc = reinterpret_cast<AggregateFunction*>(functionDefinition.get());
            if (aggregateFunc->isDistinct) {
                supportedInputsString += "DISTINCT ";
            }
            supportedInputsString += aggregateFunc->signatureToString() + "\n";
        }
        throw BinderException("Cannot match a built-in function for given function " + name +
                              (isDistinct ? "DISTINCT " : "") +
                              LogicalTypeUtils::dataTypesToString(inputTypes) +
                              ". Supported inputs are\n" + supportedInputsString);
    }
}

uint32_t BuiltInFunctions::getTargetTypeCost(LogicalTypeID typeID) {
    switch (typeID) {
    case LogicalTypeID::INT16:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT16: {
        return 110;
    }
    case LogicalTypeID::INT32: {
        return 103;
    }
    case LogicalTypeID::INT64: {
        return 101;
    }
    case LogicalTypeID::FLOAT: {
        return 110;
    }
    case LogicalTypeID::DOUBLE: {
        return 102;
    }
    case LogicalTypeID::INT128:
    case LogicalTypeID::TIMESTAMP: {
        return 120;
    }
    default: {
        // LCOV_EXCL_START
        throw InternalException("Unsupported casting operation.");
        // LCOC_EXCL_STOP
    }
    }
}

uint32_t BuiltInFunctions::castInt64(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT128:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castInt32(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castInt16(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castInt8(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castUInt64(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castUInt32(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castUInt16(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT64:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castUInt8(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT64:
    case LogicalTypeID::UINT16:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castInt128(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castDouble(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castFloat(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castDate(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::TIMESTAMP:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castSerial(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT64:
        return 0;
    default:
        return castInt64(targetTypeID);
    }
}

// When there is multiple candidates functions, e.g. double + int and double + double for input
// "1.5 + parameter", we prefer the one without any implicit casting i.e. double + double.
ScalarFunction* BuiltInFunctions::getBestMatch(std::vector<ScalarFunction*>& functionsToMatch) {
    assert(functionsToMatch.size() > 1);
    ScalarFunction* result = nullptr;
    auto cost = UNDEFINED_CAST_COST;
    for (auto& function : functionsToMatch) {
        std::unordered_set<LogicalTypeID> distinctParameterTypes;
        for (auto& parameterTypeID : function->parameterTypeIDs) {
            if (!distinctParameterTypes.contains(parameterTypeID)) {
                distinctParameterTypes.insert(parameterTypeID);
            }
        }
        if (distinctParameterTypes.size() < cost) {
            cost = distinctParameterTypes.size();
            result = function;
        }
    }
    assert(result != nullptr);
    return result;
}

uint32_t BuiltInFunctions::getFunctionCost(
    const std::vector<LogicalType>& inputTypes, ScalarFunction* function, bool isOverload) {
    if (function->isVarLength) {
        assert(function->parameterTypeIDs.size() == 1);
        return matchVarLengthParameters(inputTypes, function->parameterTypeIDs[0], isOverload);
    } else {
        return matchParameters(inputTypes, function->parameterTypeIDs, isOverload);
    }
}

uint32_t BuiltInFunctions::matchParameters(const std::vector<LogicalType>& inputTypes,
    const std::vector<LogicalTypeID>& targetTypeIDs, bool /*isOverload*/) {
    if (inputTypes.size() != targetTypeIDs.size()) {
        return UINT32_MAX;
    }
    auto cost = 0u;
    for (auto i = 0u; i < inputTypes.size(); ++i) {
        auto castCost = getCastCost(inputTypes[i].getLogicalTypeID(), targetTypeIDs[i]);
        if (castCost == UNDEFINED_CAST_COST) {
            return UINT32_MAX;
        }
        cost += castCost;
    }
    return cost;
}

uint32_t BuiltInFunctions::matchVarLengthParameters(
    const std::vector<LogicalType>& inputTypes, LogicalTypeID targetTypeID, bool /*isOverload*/) {
    auto cost = 0u;
    for (auto& inputType : inputTypes) {
        auto castCost = getCastCost(inputType.getLogicalTypeID(), targetTypeID);
        if (castCost == UNDEFINED_CAST_COST) {
            return UINT32_MAX;
        }
        cost += castCost;
    }
    return cost;
}

void BuiltInFunctions::validateNonEmptyCandidateFunctions(
    std::vector<ScalarFunction*>& candidateFunctions, const std::string& name,
    const std::vector<LogicalType>& inputTypes) {
    if (candidateFunctions.empty()) {
        std::string supportedInputsString;
        for (auto& functionDefinition : functions.at(name)) {
            auto baseScalarFunc = reinterpret_cast<BaseScalarFunction*>(functionDefinition.get());
            supportedInputsString += baseScalarFunc->signatureToString() + "\n";
        }
        throw BinderException("Cannot match a built-in function for given function " + name +
                              LogicalTypeUtils::dataTypesToString(inputTypes) +
                              ". Supported inputs are\n" + supportedInputsString);
    }
}

void BuiltInFunctions::registerComparisonFunctions() {
    functions.insert({EQUALS_FUNC_NAME, EqualsVectorFunction::getFunctionSet()});
    functions.insert({NOT_EQUALS_FUNC_NAME, NotEqualsVectorFunction::getFunctionSet()});
    functions.insert({GREATER_THAN_FUNC_NAME, GreaterThanVectorFunction::getFunctionSet()});
    functions.insert(
        {GREATER_THAN_EQUALS_FUNC_NAME, GreaterThanEqualsVectorFunction::getFunctionSet()});
    functions.insert({LESS_THAN_FUNC_NAME, LessThanVectorFunction::getFunctionSet()});
    functions.insert({LESS_THAN_EQUALS_FUNC_NAME, LessThanEqualsVectorFunction::getFunctionSet()});
}

void BuiltInFunctions::registerArithmeticFunctions() {
    functions.insert({ADD_FUNC_NAME, AddVectorFunction::getFunctionSet()});
    functions.insert({SUBTRACT_FUNC_NAME, SubtractVectorFunction::getFunctionSet()});
    functions.insert({MULTIPLY_FUNC_NAME, MultiplyVectorFunction::getFunctionSet()});
    functions.insert({DIVIDE_FUNC_NAME, DivideVectorFunction::getFunctionSet()});
    functions.insert({MODULO_FUNC_NAME, ModuloVectorFunction::getFunctionSet()});
    functions.insert({POWER_FUNC_NAME, PowerVectorFunction::getFunctionSet()});

    functions.insert({ABS_FUNC_NAME, AbsVectorFunction::getFunctionSet()});
    functions.insert({ACOS_FUNC_NAME, AcosVectorFunction::getFunctionSet()});
    functions.insert({ASIN_FUNC_NAME, AsinVectorFunction::getFunctionSet()});
    functions.insert({ATAN_FUNC_NAME, AtanVectorFunction::getFunctionSet()});
    functions.insert({ATAN2_FUNC_NAME, Atan2VectorFunction::getFunctionSet()});
    functions.insert({BITWISE_XOR_FUNC_NAME, BitwiseXorVectorFunction::getFunctionSet()});
    functions.insert({BITWISE_AND_FUNC_NAME, BitwiseAndVectorFunction::getFunctionSet()});
    functions.insert({BITWISE_OR_FUNC_NAME, BitwiseOrVectorFunction::getFunctionSet()});
    functions.insert({BITSHIFT_LEFT_FUNC_NAME, BitShiftLeftVectorFunction::getFunctionSet()});
    functions.insert({BITSHIFT_RIGHT_FUNC_NAME, BitShiftRightVectorFunction::getFunctionSet()});
    functions.insert({CBRT_FUNC_NAME, CbrtVectorFunction::getFunctionSet()});
    functions.insert({CEIL_FUNC_NAME, CeilVectorFunction::getFunctionSet()});
    functions.insert({CEILING_FUNC_NAME, CeilVectorFunction::getFunctionSet()});
    functions.insert({COS_FUNC_NAME, CosVectorFunction::getFunctionSet()});
    functions.insert({COT_FUNC_NAME, CotVectorFunction::getFunctionSet()});
    functions.insert({DEGREES_FUNC_NAME, DegreesVectorFunction::getFunctionSet()});
    functions.insert({EVEN_FUNC_NAME, EvenVectorFunction::getFunctionSet()});
    functions.insert({FACTORIAL_FUNC_NAME, FactorialVectorFunction::getFunctionSet()});
    functions.insert({FLOOR_FUNC_NAME, FloorVectorFunction::getFunctionSet()});
    functions.insert({GAMMA_FUNC_NAME, GammaVectorFunction::getFunctionSet()});
    functions.insert({LGAMMA_FUNC_NAME, LgammaVectorFunction::getFunctionSet()});
    functions.insert({LN_FUNC_NAME, LnVectorFunction::getFunctionSet()});
    functions.insert({LOG_FUNC_NAME, LogVectorFunction::getFunctionSet()});
    functions.insert({LOG2_FUNC_NAME, Log2VectorFunction::getFunctionSet()});
    functions.insert({LOG10_FUNC_NAME, LogVectorFunction::getFunctionSet()});
    functions.insert({NEGATE_FUNC_NAME, NegateVectorFunction::getFunctionSet()});
    functions.insert({PI_FUNC_NAME, PiVectorFunction::getFunctionSet()});
    functions.insert({POW_FUNC_NAME, PowerVectorFunction::getFunctionSet()});
    functions.insert({RADIANS_FUNC_NAME, RadiansVectorFunction::getFunctionSet()});
    functions.insert({ROUND_FUNC_NAME, RoundVectorFunction::getFunctionSet()});
    functions.insert({SIN_FUNC_NAME, SinVectorFunction::getFunctionSet()});
    functions.insert({SIGN_FUNC_NAME, SignVectorFunction::getFunctionSet()});
    functions.insert({SQRT_FUNC_NAME, SqrtVectorFunction::getFunctionSet()});
    functions.insert({TAN_FUNC_NAME, TanVectorFunction::getFunctionSet()});
}

void BuiltInFunctions::registerDateFunctions() {
    functions.insert({DATE_PART_FUNC_NAME, DatePartVectorFunction::getFunctionSet()});
    functions.insert({DATEPART_FUNC_NAME, DatePartVectorFunction::getFunctionSet()});
    functions.insert({DATE_TRUNC_FUNC_NAME, DateTruncVectorFunction::getFunctionSet()});
    functions.insert({DATETRUNC_FUNC_NAME, DateTruncVectorFunction::getFunctionSet()});
    functions.insert({DAYNAME_FUNC_NAME, DayNameVectorFunction::getFunctionSet()});
    functions.insert({GREATEST_FUNC_NAME, GreatestVectorFunction::getFunctionSet()});
    functions.insert({LAST_DAY_FUNC_NAME, LastDayVectorFunction::getFunctionSet()});
    functions.insert({LEAST_FUNC_NAME, LeastVectorFunction::getFunctionSet()});
    functions.insert({MAKE_DATE_FUNC_NAME, MakeDateVectorFunction::getFunctionSet()});
    functions.insert({MONTHNAME_FUNC_NAME, MonthNameVectorFunction::getFunctionSet()});
}

void BuiltInFunctions::registerTimestampFunctions() {
    functions.insert({CENTURY_FUNC_NAME, CenturyVectorFunction::getFunctionSet()});
    functions.insert({EPOCH_MS_FUNC_NAME, EpochMsVectorFunction::getFunctionSet()});
    functions.insert({TO_TIMESTAMP_FUNC_NAME, ToTimestampVectorFunction::getFunctionSet()});
}

void BuiltInFunctions::registerIntervalFunctions() {
    functions.insert({TO_YEARS_FUNC_NAME, ToYearsVectorFunction::getFunctionSet()});
    functions.insert({TO_MONTHS_FUNC_NAME, ToMonthsVectorFunction::getFunctionSet()});
    functions.insert({TO_DAYS_FUNC_NAME, ToDaysVectorFunction::getFunctionSet()});
    functions.insert({TO_HOURS_FUNC_NAME, ToHoursVectorFunction::getFunctionSet()});
    functions.insert({TO_MINUTES_FUNC_NAME, ToMinutesVectorFunction::getFunctionSet()});
    functions.insert({TO_SECONDS_FUNC_NAME, ToSecondsVectorFunction::getFunctionSet()});
    functions.insert({TO_MILLISECONDS_FUNC_NAME, ToMillisecondsVectorFunction::getFunctionSet()});
    functions.insert({TO_MICROSECONDS_FUNC_NAME, ToMicrosecondsVectorFunction::getFunctionSet()});
}

void BuiltInFunctions::registerBlobFunctions() {
    functions.insert({OCTET_LENGTH_FUNC_NAME, OctetLengthFunctions::getFunctionSet()});
    functions.insert({ENCODE_FUNC_NAME, EncodeFunctions::getFunctionSet()});
    functions.insert({DECODE_FUNC_NAME, DecodeFunctions::getFunctionSet()});
}

void BuiltInFunctions::registerStringFunctions() {
    functions.insert({ARRAY_EXTRACT_FUNC_NAME, ArrayExtractFunction::getFunctionSet()});
    functions.insert({CONCAT_FUNC_NAME, ConcatFunction::getFunctionSet()});
    functions.insert({CONTAINS_FUNC_NAME, ContainsFunction::getFunctionSet()});
    functions.insert({ENDS_WITH_FUNC_NAME, EndsWithFunction::getFunctionSet()});
    functions.insert({LCASE_FUNC_NAME, LowerFunction::getFunctionSet()});
    functions.insert({LEFT_FUNC_NAME, LeftFunction::getFunctionSet()});
    functions.insert({LOWER_FUNC_NAME, LowerFunction::getFunctionSet()});
    functions.insert({LPAD_FUNC_NAME, LpadFunction::getFunctionSet()});
    functions.insert({LTRIM_FUNC_NAME, LtrimVectorFunction::getFunctionSet()});
    functions.insert({PREFIX_FUNC_NAME, StartsWithVectorFunction::getFunctionSet()});
    functions.insert({REPEAT_FUNC_NAME, RepeatVectorFunction::getFunctionSet()});
    functions.insert({REVERSE_FUNC_NAME, ReverseVectorFunction::getFunctionSet()});
    functions.insert({RIGHT_FUNC_NAME, RightVectorFunction::getFunctionSet()});
    functions.insert({RPAD_FUNC_NAME, RpadVectorFunction::getFunctionSet()});
    functions.insert({RTRIM_FUNC_NAME, RtrimVectorFunction::getFunctionSet()});
    functions.insert({STARTS_WITH_FUNC_NAME, StartsWithVectorFunction::getFunctionSet()});
    functions.insert({SUBSTR_FUNC_NAME, SubStrVectorFunction::getFunctionSet()});
    functions.insert({SUBSTRING_FUNC_NAME, SubStrVectorFunction::getFunctionSet()});
    functions.insert({SUFFIX_FUNC_NAME, EndsWithFunction::getFunctionSet()});
    functions.insert({TRIM_FUNC_NAME, TrimVectorFunction::getFunctionSet()});
    functions.insert({UCASE_FUNC_NAME, UpperVectorFunction::getFunctionSet()});
    functions.insert({UPPER_FUNC_NAME, UpperVectorFunction::getFunctionSet()});
    functions.insert(
        {REGEXP_FULL_MATCH_FUNC_NAME, RegexpFullMatchVectorFunction::getFunctionSet()});
    functions.insert({REGEXP_MATCHES_FUNC_NAME, RegexpMatchesVectorFunction::getFunctionSet()});
    functions.insert({REGEXP_REPLACE_FUNC_NAME, RegexpReplaceVectorFunction::getFunctionSet()});
    functions.insert({REGEXP_EXTRACT_FUNC_NAME, RegexpExtractVectorFunction::getFunctionSet()});
    functions.insert(
        {REGEXP_EXTRACT_ALL_FUNC_NAME, RegexpExtractAllVectorFunction::getFunctionSet()});
}

void BuiltInFunctions::registerCastFunctions() {
    functions.insert({CAST_DATE_FUNC_NAME, CastToDateVectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_DATE_FUNC_NAME, CastToDateVectorFunction::getFunctionSet()});
    functions.insert(
        {CAST_TO_TIMESTAMP_FUNC_NAME, CastToTimestampVectorFunction::getFunctionSet()});
    functions.insert({CAST_INTERVAL_FUNC_NAME, CastToIntervalVectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_INTERVAL_FUNC_NAME, CastToIntervalVectorFunction::getFunctionSet()});
    functions.insert({CAST_STRING_FUNC_NAME, CastToStringVectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_STRING_FUNC_NAME, CastToStringVectorFunction::getFunctionSet()});
    functions.insert({CAST_BLOB_FUNC_NAME, CastToBlobVectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_BLOB_FUNC_NAME, CastToBlobVectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_DOUBLE_FUNC_NAME, CastToDoubleVectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_FLOAT_FUNC_NAME, CastToFloatVectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_SERIAL_FUNC_NAME, CastToSerialVectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_INT64_FUNC_NAME, CastToInt64VectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_INT32_FUNC_NAME, CastToInt32VectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_INT16_FUNC_NAME, CastToInt16VectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_INT8_FUNC_NAME, CastToInt8VectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_UINT64_FUNC_NAME, CastToUInt64VectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_UINT32_FUNC_NAME, CastToUInt32VectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_UINT16_FUNC_NAME, CastToUInt16VectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_UINT8_FUNC_NAME, CastToUInt8VectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_INT128_FUNC_NAME, CastToInt128VectorFunction::getFunctionSet()});
    functions.insert({CAST_TO_BOOL_FUNC_NAME, CastToBoolVectorFunction::getFunctionSet()});
}

void BuiltInFunctions::registerListFunctions() {
    functions.insert({LIST_CREATION_FUNC_NAME, ListCreationVectorFunction::getFunctionSet()});
    functions.insert({LIST_RANGE_FUNC_NAME, ListRangeVectorFunction::getFunctionSet()});
    functions.insert({SIZE_FUNC_NAME, SizeVectorFunction::getFunctionSet()});
    functions.insert({LIST_EXTRACT_FUNC_NAME, ListExtractVectorFunction::getFunctionSet()});
    functions.insert({LIST_ELEMENT_FUNC_NAME, ListExtractVectorFunction::getFunctionSet()});
    functions.insert({LIST_CONCAT_FUNC_NAME, ListConcatVectorFunction::getFunctionSet()});
    functions.insert({LIST_CAT_FUNC_NAME, ListConcatVectorFunction::getFunctionSet()});
    functions.insert({ARRAY_CONCAT_FUNC_NAME, ListConcatVectorFunction::getFunctionSet()});
    functions.insert({ARRAY_CAT_FUNC_NAME, ListConcatVectorFunction::getFunctionSet()});
    functions.insert({LIST_APPEND_FUNC_NAME, ListAppendVectorFunction::getFunctionSet()});
    functions.insert({ARRAY_APPEND_FUNC_NAME, ListAppendVectorFunction::getFunctionSet()});
    functions.insert({ARRAY_PUSH_BACK_FUNC_NAME, ListAppendVectorFunction::getFunctionSet()});
    functions.insert({LIST_PREPEND_FUNC_NAME, ListPrependVectorFunction::getFunctionSet()});
    functions.insert({ARRAY_PREPEND_FUNC_NAME, ListPrependVectorFunction::getFunctionSet()});
    functions.insert({ARRAY_PUSH_FRONT_FUNC_NAME, ListPrependVectorFunction::getFunctionSet()});
    functions.insert({LIST_POSITION_FUNC_NAME, ListPositionVectorFunction::getFunctionSet()});
    functions.insert({ARRAY_POSITION_FUNC_NAME, ListPositionVectorFunction::getFunctionSet()});
    functions.insert({LIST_INDEXOF_FUNC_NAME, ListPositionVectorFunction::getFunctionSet()});
    functions.insert({ARRAY_INDEXOF_FUNC_NAME, ListPositionVectorFunction::getFunctionSet()});
    functions.insert({LIST_CONTAINS_FUNC_NAME, ListContainsVectorFunction::getFunctionSet()});
    functions.insert({LIST_HAS_FUNC_NAME, ListContainsVectorFunction::getFunctionSet()});
    functions.insert({ARRAY_CONTAINS_FUNC_NAME, ListContainsVectorFunction::getFunctionSet()});
    functions.insert({ARRAY_HAS_FUNC_NAME, ListContainsVectorFunction::getFunctionSet()});
    functions.insert({LIST_SLICE_FUNC_NAME, ListSliceVectorFunction::getFunctionSet()});
    functions.insert({ARRAY_SLICE_FUNC_NAME, ListSliceVectorFunction::getFunctionSet()});
    functions.insert({LIST_SORT_FUNC_NAME, ListSortVectorFunction::getFunctionSet()});
    functions.insert(
        {LIST_REVERSE_SORT_FUNC_NAME, ListReverseSortVectorFunction::getFunctionSet()});
    functions.insert({LIST_SUM_FUNC_NAME, ListSumVectorFunction::getFunctionSet()});
    functions.insert({LIST_PRODUCT_FUNC_NAME, ListProductVectorFunction::getFunctionSet()});
    functions.insert({LIST_DISTINCT_FUNC_NAME, ListDistinctVectorFunction::getFunctionSet()});
    functions.insert({LIST_UNIQUE_FUNC_NAME, ListUniqueVectorFunction::getFunctionSet()});
    functions.insert({LIST_ANY_VALUE_FUNC_NAME, ListAnyValueVectorFunction::getFunctionSet()});
}

void BuiltInFunctions::registerStructFunctions() {
    functions.insert({STRUCT_PACK_FUNC_NAME, StructPackFunctions::getFunctionSet()});
    functions.insert({STRUCT_EXTRACT_FUNC_NAME, StructExtractFunctions::getFunctionSet()});
}

void BuiltInFunctions::registerMapFunctions() {
    functions.insert({MAP_CREATION_FUNC_NAME, MapCreationFunctions::getFunctionSet()});
    functions.insert({MAP_EXTRACT_FUNC_NAME, MapExtractFunctions::getFunctionSet()});
    functions.insert({ELEMENT_AT_FUNC_NAME, MapExtractFunctions::getFunctionSet()});
    functions.insert({CARDINALITY_FUNC_NAME, SizeVectorFunction::getFunctionSet()});
    functions.insert({MAP_KEYS_FUNC_NAME, MapKeysFunctions::getFunctionSet()});
    functions.insert({MAP_VALUES_FUNC_NAME, MapValuesFunctions::getFunctionSet()});
}

void BuiltInFunctions::registerUnionFunctions() {
    functions.insert({UNION_VALUE_FUNC_NAME, UnionValueVectorFunction::getFunctionSet()});
    functions.insert({UNION_TAG_FUNC_NAME, UnionTagVectorFunction::getFunctionSet()});
    functions.insert({UNION_EXTRACT_FUNC_NAME, UnionExtractVectorFunction::getFunctionSet()});
}

void BuiltInFunctions::registerNodeRelFunctions() {
    functions.insert({OFFSET_FUNC_NAME, OffsetVectorFunction::getFunctionSet()});
}

void BuiltInFunctions::registerPathFunctions() {
    functions.insert({NODES_FUNC_NAME, NodesVectorFunction::getFunctionSet()});
    functions.insert({RELS_FUNC_NAME, RelsVectorFunction::getFunctionSet()});
    functions.insert({PROPERTIES_FUNC_NAME, PropertiesVectorFunction::getFunctionSet()});
    functions.insert({IS_TRAIL_FUNC_NAME, IsTrailVectorFunction::getFunctionSet()});
    functions.insert({IS_ACYCLIC_FUNC_NAME, IsACyclicVectorFunction::getFunctionSet()});
}

void BuiltInFunctions::registerCountStar() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<AggregateFunction>(COUNT_STAR_FUNC_NAME,
        std::vector<common::LogicalTypeID>{}, LogicalTypeID::INT64, CountStarFunction::initialize,
        CountStarFunction::updateAll, CountStarFunction::updatePos, CountStarFunction::combine,
        CountStarFunction::finalize, false));
    functions.insert({COUNT_STAR_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerCount() {
    function_set functionSet;
    for (auto& type : LogicalTypeUtils::getAllValidLogicTypes()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(AggregateFunctionUtil::getAggFunc<CountFunction>(COUNT_FUNC_NAME,
                type.getLogicalTypeID(), LogicalTypeID::INT64, isDistinct,
                CountFunction::paramRewriteFunc));
        }
    }
    functions.insert({COUNT_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerSum() {
    function_set functionSet;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(
                AggregateFunctionUtil::getSumFunc(SUM_FUNC_NAME, typeID, typeID, isDistinct));
        }
    }
    functions.insert({SUM_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerAvg() {
    function_set functionSet;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(AggregateFunctionUtil::getAvgFunc(
                AVG_FUNC_NAME, typeID, LogicalTypeID::DOUBLE, isDistinct));
        }
    }
    functions.insert({AVG_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerMin() {
    function_set functionSet;
    for (auto& type : LogicalTypeUtils::getAllValidComparableLogicalTypes()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(AggregateFunctionUtil::getMinFunc(type, isDistinct));
        }
    }
    functions.insert({MIN_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerMax() {
    function_set functionSet;
    for (auto& type : LogicalTypeUtils::getAllValidComparableLogicalTypes()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(AggregateFunctionUtil::getMaxFunc(type, isDistinct));
        }
    }
    functions.insert({MAX_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerCollect() {
    function_set functionSet;
    for (auto isDistinct : std::vector<bool>{true, false}) {
        functionSet.push_back(std::make_unique<AggregateFunction>(COLLECT_FUNC_NAME,
            std::vector<common::LogicalTypeID>{common::LogicalTypeID::ANY}, LogicalTypeID::VAR_LIST,
            CollectFunction::initialize, CollectFunction::updateAll, CollectFunction::updatePos,
            CollectFunction::combine, CollectFunction::finalize, isDistinct,
            CollectFunction::bindFunc));
    }
    functions.insert({COLLECT_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerTableFunctions() {
    functions.insert({CURRENT_SETTING_FUNC_NAME, CurrentSettingFunction::getFunctionSet()});
    functions.insert({DB_VERSION_FUNC_NAME, DBVersionFunction::getFunctionSet()});
    functions.insert({SHOW_TABLES_FUNC_NAME, ShowTablesFunction::getFunctionSet()});
    functions.insert({TABLE_INFO_FUNC_NAME, TableInfoFunction::getFunctionSet()});
    functions.insert({SHOW_CONNECTION_FUNC_NAME, ShowConnectionFunction::getFunctionSet()});
}

void BuiltInFunctions::addFunction(std::string name, function::function_set definitions) {
    if (functions.contains(name)) {
        throw CatalogException{stringFormat("function {} already exists.", name)};
    }
    functions.emplace(std::move(name), std::move(definitions));
}

} // namespace function
} // namespace kuzu
