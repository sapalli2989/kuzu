#pragma once

#include "function/scalar_function.h"
#include "interval_functions.h"

namespace kuzu {
namespace function {

class VectorIntervalFunction : public VectorFunction {
public:
    template<class OPERATION>
    static inline function_set getUnaryIntervalFunctionDefintion(std::string funcName) {
        function_set result;
        result.push_back(std::make_unique<ScalarFunction>(funcName,
            std::vector<common::LogicalTypeID>{common::LogicalTypeID::INT64},
            common::LogicalTypeID::INTERVAL,
            UnaryExecFunction<int64_t, common::interval_t, OPERATION>));
        return result;
    }
};

struct ToYearsVectorFunction : public VectorIntervalFunction {
    static inline function_set getFunctionSet() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToYears>(
            common::TO_YEARS_FUNC_NAME);
    }
};

struct ToMonthsVectorFunction : public VectorIntervalFunction {
    static inline function_set getFunctionSet() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToMonths>(
            common::TO_MONTHS_FUNC_NAME);
    }
};

struct ToDaysVectorFunction : public VectorIntervalFunction {
    static inline function_set getFunctionSet() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToDays>(
            common::TO_DAYS_FUNC_NAME);
    }
};

struct ToHoursVectorFunction : public VectorIntervalFunction {
    static inline function_set getFunctionSet() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToHours>(
            common::TO_HOURS_FUNC_NAME);
    }
};

struct ToMinutesVectorFunction : public VectorIntervalFunction {
    static inline function_set getFunctionSet() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToMinutes>(
            common::TO_MINUTES_FUNC_NAME);
    }
};

struct ToSecondsVectorFunction : public VectorIntervalFunction {
    static inline function_set getFunctionSet() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToSeconds>(
            common::TO_SECONDS_FUNC_NAME);
    }
};

struct ToMillisecondsVectorFunction : public VectorIntervalFunction {
    static inline function_set getFunctionSet() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToMilliseconds>(
            common::TO_MILLISECONDS_FUNC_NAME);
    }
};

struct ToMicrosecondsVectorFunction : public VectorIntervalFunction {
    static inline function_set getFunctionSet() {
        return VectorIntervalFunction::getUnaryIntervalFunctionDefintion<ToMicroseconds>(
            common::TO_MICROSECONDS_FUNC_NAME);
    }
};

} // namespace function
} // namespace kuzu
