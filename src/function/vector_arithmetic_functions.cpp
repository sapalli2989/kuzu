#include "function/arithmetic/vector_arithmetic_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

function_set AddVectorFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<Add>(ADD_FUNC_NAME, typeID));
    }
    // interval + interval → interval
    result.push_back(getBinaryDefinition<Add, interval_t, interval_t>(
        ADD_FUNC_NAME, LogicalTypeID::INTERVAL, LogicalTypeID::INTERVAL));
    // date + int → date
    result.push_back(make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT64}, LogicalTypeID::DATE,
        BinaryExecFunction<date_t, int64_t, date_t, Add>));
    // int + date → date
    result.push_back(make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64, LogicalTypeID::DATE}, LogicalTypeID::DATE,
        BinaryExecFunction<int64_t, date_t, date_t, Add>));
    // date + interval → date
    result.push_back(make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INTERVAL},
        LogicalTypeID::DATE, BinaryExecFunction<date_t, interval_t, date_t, Add>));
    // interval + date → date
    result.push_back(make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::DATE},
        LogicalTypeID::DATE, BinaryExecFunction<interval_t, date_t, date_t, Add>));
    // timestamp + interval → timestamp
    result.push_back(make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL},
        LogicalTypeID::TIMESTAMP, BinaryExecFunction<timestamp_t, interval_t, timestamp_t, Add>));
    // interval + timestamp → timestamp
    result.push_back(make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::TIMESTAMP},
        LogicalTypeID::TIMESTAMP, BinaryExecFunction<interval_t, timestamp_t, timestamp_t, Add>));
    return result;
}

function_set SubtractVectorFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<Subtract>(SUBTRACT_FUNC_NAME, typeID));
    }
    // date - date → int64
    result.push_back(getBinaryDefinition<Subtract, date_t, int64_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::DATE, LogicalTypeID::INT64));
    // date - integer → date
    result.push_back(make_unique<ScalarFunction>(SUBTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT64}, LogicalTypeID::DATE,
        BinaryExecFunction<date_t, int64_t, date_t, Subtract>));
    // date - interval → date
    result.push_back(make_unique<ScalarFunction>(SUBTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INTERVAL},
        LogicalTypeID::DATE, BinaryExecFunction<date_t, interval_t, date_t, Subtract>));
    // timestamp - timestamp → interval
    result.push_back(getBinaryDefinition<Subtract, timestamp_t, interval_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL));
    // timestamp - interval → timestamp
    result.push_back(make_unique<ScalarFunction>(SUBTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL},
        LogicalTypeID::TIMESTAMP,
        BinaryExecFunction<timestamp_t, interval_t, timestamp_t, Subtract>));
    // interval - interval → interval
    result.push_back(getBinaryDefinition<Subtract, interval_t, interval_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::INTERVAL, LogicalTypeID::INTERVAL));
    return result;
}

function_set MultiplyVectorFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<Multiply>(MULTIPLY_FUNC_NAME, typeID));
    }
    return result;
}

function_set DivideVectorFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<Divide>(DIVIDE_FUNC_NAME, typeID));
    }
    // interval / int → interval
    result.push_back(make_unique<ScalarFunction>(DIVIDE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::INT64},
        LogicalTypeID::INTERVAL, BinaryExecFunction<interval_t, int64_t, interval_t, Divide>));
    return result;
}

function_set ModuloVectorFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<Modulo>(MODULO_FUNC_NAME, typeID));
    }
    return result;
}

function_set PowerVectorFunction::getFunctionSet() {
    function_set result;
    // double_t ^ double_t -> double_t
    result.push_back(getBinaryDefinition<Power, double_t>(
        POWER_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set NegateVectorFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryDefinition<Negate>(NEGATE_FUNC_NAME, typeID));
    }
    return result;
}

function_set AbsVectorFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryDefinition<Abs>(ABS_FUNC_NAME, typeID));
    }
    return result;
}

function_set FloorVectorFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryDefinition<Floor>(FLOOR_FUNC_NAME, typeID));
    }
    return result;
}

function_set CeilVectorFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryDefinition<Ceil>(CEIL_FUNC_NAME, typeID));
    }
    return result;
}

function_set SinVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Sin, double_t>(
        SIN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set CosVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Cos, double_t>(
        COS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set TanVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Tan, double_t>(
        TAN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set CotVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Cot, double_t>(
        COT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set AsinVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Asin, double_t>(
        ASIN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set AcosVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Acos, double_t>(
        ACOS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set AtanVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Atan, double_t>(
        ATAN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set FactorialVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(FACTORIAL_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64}, LogicalTypeID::INT64,
        UnaryExecFunction<int64_t, int64_t, Factorial>));
    return result;
}

function_set SqrtVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Sqrt, double_t>(
        SQRT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set CbrtVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Cbrt, double_t>(
        CBRT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set GammaVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Gamma, double_t>(
        GAMMA_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set LgammaVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Lgamma, double_t>(
        LGAMMA_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set LnVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Ln, double_t>(
        LN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set LogVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Log, double_t>(
        LOG_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set Log2VectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Log2, double_t>(
        LOG2_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set DegreesVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Degrees, double_t>(
        DEGREES_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set RadiansVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Radians, double_t>(
        RADIANS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set EvenVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Even, double_t>(
        EVEN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set SignVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryDefinition<Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    result.push_back(getUnaryDefinition<Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::INT64));
    result.push_back(getUnaryDefinition<Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::FLOAT, LogicalTypeID::INT64));
    return result;
}

function_set Atan2VectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getBinaryDefinition<Atan2, double_t>(
        ATAN2_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set RoundVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(ROUND_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DOUBLE, LogicalTypeID::INT64},
        LogicalTypeID::DOUBLE, BinaryExecFunction<double_t, int64_t, double_t, Round>));
    return result;
}

function_set BitwiseXorVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getBinaryDefinition<BitwiseXor, int64_t>(
        BITWISE_XOR_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitwiseAndVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getBinaryDefinition<BitwiseAnd, int64_t>(
        BITWISE_AND_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitwiseOrVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getBinaryDefinition<BitwiseOr, int64_t>(
        BITWISE_OR_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitShiftLeftVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getBinaryDefinition<BitShiftLeft, int64_t>(
        BITSHIFT_LEFT_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitShiftRightVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getBinaryDefinition<BitShiftRight, int64_t>(
        BITSHIFT_RIGHT_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set PiVectorFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(PI_FUNC_NAME, std::vector<LogicalTypeID>{},
        LogicalTypeID::DOUBLE, ConstExecFunction<double_t, Pi>));
    return result;
}

} // namespace function
} // namespace kuzu
