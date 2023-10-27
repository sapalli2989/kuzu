#pragma once

#include "arithmetic_functions.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

class VectorArithmeticFunction : public VectorFunction {
public:
    template<typename FUNC>
    static std::unique_ptr<ScalarFunction> getUnaryDefinition(
        std::string name, common::LogicalTypeID operandTypeID) {
        function::scalar_exec_func execFunc;
        getUnaryExecFunc<FUNC>(operandTypeID, execFunc);
        return std::make_unique<ScalarFunction>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID}, operandTypeID, execFunc);
    }

    template<typename FUNC, typename OPERAND_TYPE, typename RETURN_TYPE = OPERAND_TYPE>
    static std::unique_ptr<ScalarFunction> getUnaryDefinition(
        std::string name, common::LogicalTypeID operandTypeID, common::LogicalTypeID resultTypeID) {
        return std::make_unique<ScalarFunction>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID}, resultTypeID,
            UnaryExecFunction<OPERAND_TYPE, RETURN_TYPE, FUNC>);
    }

    template<typename FUNC>
    static inline std::unique_ptr<ScalarFunction> getBinaryDefinition(
        std::string name, common::LogicalTypeID operandTypeID) {
        function::scalar_exec_func execFunc;
        getBinaryExecFunc<FUNC>(operandTypeID, execFunc);
        return std::make_unique<ScalarFunction>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID, operandTypeID}, operandTypeID,
            execFunc);
    }

    template<typename FUNC, typename OPERAND_TYPE, typename RETURN_TYPE = OPERAND_TYPE>
    static inline std::unique_ptr<ScalarFunction> getBinaryDefinition(
        std::string name, common::LogicalTypeID operandTypeID, common::LogicalTypeID resultTypeID) {
        return std::make_unique<ScalarFunction>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID, operandTypeID}, resultTypeID,
            BinaryExecFunction<OPERAND_TYPE, OPERAND_TYPE, RETURN_TYPE, FUNC>);
    }

private:
    template<typename FUNC>
    static void getUnaryExecFunc(common::LogicalTypeID operandTypeID, scalar_exec_func& func) {
        switch (operandTypeID) {
        case common::LogicalTypeID::SERIAL:
        case common::LogicalTypeID::INT64: {
            func = UnaryExecFunction<int64_t, int64_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT32: {
            func = UnaryExecFunction<int32_t, int32_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT16: {
            func = UnaryExecFunction<int16_t, int16_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT8: {
            func = UnaryExecFunction<int8_t, int8_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::UINT64: {
            func = UnaryExecFunction<uint64_t, uint64_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::UINT32: {
            func = UnaryExecFunction<uint32_t, uint32_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::UINT16: {
            func = UnaryExecFunction<uint16_t, uint16_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::UINT8: {
            func = UnaryExecFunction<uint8_t, uint8_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT128: {
            func = UnaryExecFunction<kuzu::common::int128_t, kuzu::common::int128_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::DOUBLE: {
            func = UnaryExecFunction<double_t, double_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::FLOAT: {
            func = UnaryExecFunction<float_t, float_t, FUNC>;
            return;
        }
        default:
            throw common::RuntimeException(
                "Invalid input data types(" +
                common::LogicalTypeUtils::dataTypeToString(operandTypeID) +
                ") for getUnaryExecFunc.");
        }
    }

    template<typename FUNC>
    static void getBinaryExecFunc(common::LogicalTypeID operandTypeID, scalar_exec_func& func) {
        switch (operandTypeID) {
        case common::LogicalTypeID::SERIAL:
        case common::LogicalTypeID::INT64: {
            func = BinaryExecFunction<int64_t, int64_t, int64_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT32: {
            func = BinaryExecFunction<int32_t, int32_t, int32_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT16: {
            func = BinaryExecFunction<int16_t, int16_t, int16_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT8: {
            func = BinaryExecFunction<int8_t, int8_t, int8_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::UINT64: {
            func = BinaryExecFunction<uint64_t, uint64_t, uint64_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::UINT32: {
            func = BinaryExecFunction<uint32_t, uint32_t, uint32_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::UINT16: {
            func = BinaryExecFunction<uint16_t, uint16_t, uint16_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::UINT8: {
            func = BinaryExecFunction<uint8_t, uint8_t, uint8_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT128: {
            func = BinaryExecFunction<kuzu::common::int128_t, kuzu::common::int128_t,
                kuzu::common::int128_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::DOUBLE: {
            func = BinaryExecFunction<double_t, double_t, double_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::FLOAT: {
            func = BinaryExecFunction<float_t, float_t, float_t, FUNC>;
            return;
        }
        default:
            throw common::RuntimeException(
                "Invalid input data types(" +
                common::LogicalTypeUtils::dataTypeToString(operandTypeID) +
                ") for getBinaryExecFunc.");
        }
    }
};

struct AddVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct SubtractVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct MultiplyVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct DivideVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct ModuloVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct PowerVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct AbsVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct AcosVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct AsinVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct AtanVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct Atan2VectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct BitwiseXorVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct BitwiseAndVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct BitwiseOrVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct BitShiftLeftVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct BitShiftRightVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct CbrtVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct CeilVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct CosVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct CotVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct DegreesVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct EvenVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct FactorialVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct FloorVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct GammaVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct LgammaVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct LnVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct LogVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct Log2VectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct NegateVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct PiVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct RadiansVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct RoundVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct SinVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct SignVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct SqrtVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

struct TanVectorFunction : public VectorArithmeticFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
