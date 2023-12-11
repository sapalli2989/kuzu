#include <cstring>
#include <type_traits>
#include <utility>

#include "common/assert.h"

namespace kuzu {
namespace common {

template<typename T>
class MaybeUninit {
    std::aligned_storage_t<sizeof(T), alignof(T)> data;

public:
    T& assumeInit() { return *reinterpret_cast<T*>(&data); }
    const T& assumeInit() const { return *reinterpret_cast<const T*>(&data); }
};

template<typename T, size_t N>
class StaticVector {
    MaybeUninit<T> items[N];
    size_t len;

public:
    StaticVector() : len(0){};
    StaticVector(const StaticVector& other) : len(other.len) {
        // Element-wise copy.
        for (auto i = 0u; i < len; i++) {
            new (&this->operator[](i)) T(other[i]);
        }
    }
    StaticVector(StaticVector&& other) : len(other.len) {
        // SAFETY: since we set other.len to 0, none of the other elements will be destroyed.
        std::memcpy(items, other.items, len * sizeof(T));
        other.len = 0;
    }
    StaticVector& operator=(const StaticVector& other) {
        StaticVector clone(other);
        return *this = std::move(other);
    }
    StaticVector& operator=(StaticVector&& other) {
        if (&other != this) {
            clear();
            std::memcpy(items, other.items, len * sizeof(T));
            other.len = 0;
        }
        return *this;
    }
    ~StaticVector() { clear(); }

    T& operator[](size_t i) { return items[i].assumeInit(); }
    const T& operator[](size_t i) const { return items[i].assumeInit(); }
    void push_back(T elem) {
        KU_ASSERT(len < N);
        new (&this->operator[](len)) T(std::move(elem));
        len++;
    }
    T pop_back() {
        KU_ASSERT(len > 0);
        len--;
        return std::move(this->operator[](len));
    }
    void clear() {
        for (auto i = 0u; i < len; i++) {
            this->operator[](i).~T();
        }
        len = 0;
    }

    bool empty() const { return len == 0; }
    bool full() const { return len == N; }
    size_t size() const { return len; }
};

} // namespace common
} // namespace kuzu
