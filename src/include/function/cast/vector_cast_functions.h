#pragma once

#include "function/cast/functions/cast_functions.h"
#include "function/cast/functions/cast_string_to_functions.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

/**
 *  In the system we define explicit cast and implicit cast.
 *  Explicit casts are performed from user function calls, e.g. date(), string().
 *  Implicit casts are added internally.
 */
class VectorCastFunction : public VectorFunction {
public:
    // This function is only used by expression binder when implicit cast is needed.
    // The expression binder should consider reusing the existing matchFunction() API.
    static bool hasImplicitCast(
        const common::LogicalType& srcType, const common::LogicalType& dstType);
    static std::string bindImplicitCastFuncName(const common::LogicalType& dstType);
    static void bindImplicitCastFunc(common::LogicalTypeID sourceTypeID,
        common::LogicalTypeID targetTypeID, scalar_exec_func& func);

protected:
    template<typename TARGET_TYPE, typename FUNC>
    inline static std::unique_ptr<ScalarFunction> bindNumericCastVectorFunction(
        const std::string& funcName, common::LogicalTypeID sourceTypeID,
        common::LogicalTypeID targetTypeID) {
        scalar_exec_func func;
        bindImplicitNumericalCastFunc<TARGET_TYPE, FUNC>(sourceTypeID, func);
        return std::make_unique<ScalarFunction>(
            funcName, std::vector<common::LogicalTypeID>{sourceTypeID}, targetTypeID, func);
    }

    template<typename DST_TYPE, typename OP>
    static void bindImplicitNumericalCastFunc(
        common::LogicalTypeID srcTypeID, scalar_exec_func& func) {
        switch (srcTypeID) {
        case common::LogicalTypeID::INT8: {
            func = UnaryExecFunction<int8_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::INT16: {
            func = UnaryExecFunction<int16_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::INT32: {
            func = UnaryExecFunction<int32_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::SERIAL:
        case common::LogicalTypeID::INT64: {
            func = UnaryExecFunction<int64_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::UINT8: {
            func = UnaryExecFunction<uint8_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::UINT16: {
            func = UnaryExecFunction<uint16_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::UINT32: {
            func = UnaryExecFunction<uint32_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::UINT64: {
            func = UnaryExecFunction<uint64_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::INT128: {
            func = UnaryExecFunction<common::int128_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::FLOAT: {
            func = UnaryExecFunction<float_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::DOUBLE: {
            func = UnaryExecFunction<double_t, DST_TYPE, OP>;
            return;
        }
        default:
            throw common::NotImplementedException(
                "Unimplemented casting Function from " +
                common::LogicalTypeUtils::dataTypeToString(srcTypeID) + " to numeric.");
        }
    }

    template<typename TARGET_TYPE>
    inline static std::unique_ptr<ScalarFunction> bindCastStringToVectorFunction(
        const std::string& funcName, common::LogicalTypeID targetTypeID) {
        return std::make_unique<ScalarFunction>(funcName,
            std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING}, targetTypeID,
            UnaryStringExecFunction<common::ku_string_t, TARGET_TYPE, CastStringToTypes>);
    }
};

struct CastToDateVectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToTimestampVectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToIntervalVectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToStringVectorFunction : public VectorCastFunction {
    static void getUnaryCastToStringExecFunction(
        common::LogicalTypeID typeID, scalar_exec_func& func);
    static function_set getFunctionSet();
};

struct CastToBlobVectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToBoolVectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToDoubleVectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToFloatVectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToSerialVectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToInt128VectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToInt64VectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToInt32VectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToInt16VectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToInt8VectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToUInt64VectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToUInt32VectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToUInt16VectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

struct CastToUInt8VectorFunction : public VectorCastFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
