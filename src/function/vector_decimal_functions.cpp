#include "function/decimal/vector_decimal_functions.h"


#include "function/arithmetic/add.h"
#include "function/arithmetic/subtract.h"
#include "function/arithmetic/modulo.h"
#include "function/cast/functions/numeric_limits.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

using param_get_func_t = std::function<std::pair<int, int>(int, int, int, int)>;

template<typename T>
static constexpr std::array<T, NumericLimits<T>::digits()> pow10Sequence() {
    std::array<T, NumericLimits<T>::digits()> retval;
    retval[0] = 1;
    for (auto i = 1u; i < NumericLimits<T>::digits(); i++) {
        retval[i] = retval[i-1] * 10;
    }
    return retval;
}

template<>
constexpr std::array<int128_t, NumericLimits<int128_t>::digits()> pow10Sequence() {
    return {
        int128_t(1UL, 0LL),
		int128_t(10UL, 0LL),
		int128_t(100UL, 0LL),
		int128_t(1000UL, 0LL),
		int128_t(10000UL, 0LL),
		int128_t(100000UL, 0LL),
		int128_t(1000000UL, 0LL),
		int128_t(10000000UL, 0LL),
		int128_t(100000000UL, 0LL),
		int128_t(1000000000UL, 0LL),
		int128_t(10000000000UL, 0LL),
		int128_t(100000000000UL, 0LL),
		int128_t(1000000000000UL, 0LL),
		int128_t(10000000000000UL, 0LL),
		int128_t(100000000000000UL, 0LL),
		int128_t(1000000000000000UL, 0LL),
		int128_t(10000000000000000UL, 0LL),
		int128_t(100000000000000000UL, 0LL),
		int128_t(1000000000000000000UL, 0LL),
		int128_t(10000000000000000000UL, 0LL),
		int128_t(7766279631452241920UL, 5LL),
		int128_t(3875820019684212736UL, 54LL),
		int128_t(1864712049423024128UL, 542LL),
		int128_t(200376420520689664UL, 5421LL),
		int128_t(2003764205206896640UL, 54210LL),
		int128_t(1590897978359414784UL, 542101LL),
		int128_t(15908979783594147840UL, 5421010LL),
		int128_t(11515845246265065472UL, 54210108LL),
		int128_t(4477988020393345024UL, 542101086LL),
		int128_t(7886392056514347008UL, 5421010862LL),
		int128_t(5076944270305263616UL, 54210108624LL),
		int128_t(13875954555633532928UL, 542101086242LL),
		int128_t(9632337040368467968UL, 5421010862427LL),
		int128_t(4089650035136921600UL, 54210108624275LL),
		int128_t(4003012203950112768UL, 542101086242752LL),
		int128_t(3136633892082024448UL, 5421010862427522LL),
		int128_t(12919594847110692864UL, 54210108624275221LL),
		int128_t(68739955140067328UL, 542101086242752217LL),
        int128_t(687399551400673280UL, 5421010862427522170LL),
    }; // couldn't find a clean way to do this
}

struct DecimalAdd {
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result, common::ValueVector&) {
        result = left + right;
    }
};

struct DecimalSubtract {
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result, common::ValueVector&) {
        result = left - right;
    }
};

struct DecimalMultiply {
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result, common::ValueVector& resultValueVector) {
        constexpr auto pow10s = pow10Sequence<R>();
        result = left * right / pow10s[DecimalType::getScale(resultValueVector.dataType)];
    }
};

struct DecimalDivide {
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result, common::ValueVector& resultValueVector) {
        constexpr auto pow10s = pow10Sequence<R>();
        result = left * pow10s[DecimalType::getScale(resultValueVector.dataType)] / right;
    }
};

struct DecimalModulo {
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result, common::ValueVector&) {
        result = left % right;   
    }
};

template<typename FUNC>
static std::unique_ptr<FunctionBindData> genericArithmeticFunc(
    const binder::expression_vector& arguments, Function* func, param_get_func_t getParams) {
    auto asScalar = ku_dynamic_cast<Function*, ScalarFunction*>(func);
    KU_ASSERT(asScalar != nullptr);
    auto resultingType = arguments[0]->getDataType().copy();
    if (arguments[0]->getDataType() != arguments[1]->getDataType()) {
        auto precision1 = DecimalType::getPrecision(arguments[0]->getDataType());
        auto precision2 = DecimalType::getPrecision(arguments[1]->getDataType());
        auto scale1 = DecimalType::getScale(arguments[0]->getDataType());
        auto scale2 = DecimalType::getScale(arguments[1]->getDataType());
        auto params = getParams(precision1, precision2, scale1, scale2);
        resultingType = LogicalType::DECIMAL(params.first, params.second);
    }
    switch (resultingType->getPhysicalType()) {
    case PhysicalTypeID::INT16:
        asScalar->execFunc = ScalarFunction::BinaryStringExecFunction<int16_t, int16_t, int16_t, FUNC>;
        break;
    case PhysicalTypeID::INT32:
        asScalar->execFunc = ScalarFunction::BinaryStringExecFunction<int32_t, int32_t, int32_t, FUNC>;
        break;
    case PhysicalTypeID::INT64:
        asScalar->execFunc = ScalarFunction::BinaryStringExecFunction<int64_t, int64_t, int64_t, FUNC>;
        break;
    case PhysicalTypeID::INT128:
        asScalar->execFunc = ScalarFunction::BinaryStringExecFunction<int128_t, int128_t, int128_t, FUNC>;
        break;
    default:
        KU_UNREACHABLE;
    }
    return std::make_unique<FunctionBindData>(std::vector<LogicalType>{*resultingType, *resultingType}, std::move(resultingType));
}

// resulting param func rules are from
// https://learn.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql
static std::pair<int, int> resultingAddParams(int p1, int p2, int s1, int s2) {
    auto p = std::min(38, std::max(s1, s2) + std::max(p1 - s1, p2 - s2) + 1);
    auto s = std::min(p, std::max(s1, s2));
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindAddFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericArithmeticFunc<DecimalAdd>(arguments, func, resultingAddParams);
}

static std::pair<int, int> resultingSubtractParams(int p1, int p2, int s1, int s2) {
    auto p = std::min(38, std::max(s1, s2) + std::max(p1 - s1, p2 - s2) + 1);
    auto s = std::min(p, std::max(s1, s2));
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindSubtractFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericArithmeticFunc<DecimalSubtract>(arguments, func, resultingSubtractParams);
}

static std::pair<int, int> resultingMultiplyParams(int p1, int p2, int s1, int s2) {
    auto p = std::min(38, p1 + p2 + 1);
    auto s = std::min(p, s1 + s2);
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindMultiplyFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericArithmeticFunc<DecimalMultiply>(arguments, func, resultingMultiplyParams);
}

static std::pair<int, int> resultingDivideParams(int p1, int p2, int s1, int s2) {
    auto p = std::min(38, p1 - s1 + s2 + std::max(6, s1 + p2 + 1));
    auto s = std::min(p, std::max(6, s1 + p2 + 1));
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindDivideFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericArithmeticFunc<DecimalDivide>(arguments, func, resultingDivideParams);
}

static std::pair<int, int> resultingModuloParams(int p1, int p2, int s1, int s2) {
    auto p = std::min(38, std::min(p1 - s1, p2 - s2) + std::max(s1, s2));
    auto s = std::min(p, std::max(s1, s2));
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindModuloFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericArithmeticFunc<DecimalModulo>(arguments, func, resultingModuloParams);
}

} // namespace function
} // namespace kuzu
