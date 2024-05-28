#pragma once

#include "common/string_utils.h"
#include "common/type_utils.h"
#include "storage/index/in_mem_hash_index.h"

namespace kuzu {
namespace storage {

enum class HashIndexLocalLookupState : uint8_t { KEY_FOUND, KEY_DELETED, KEY_NOT_EXIST };

// Local storage consists of two in memory indexes. One (localInsertionIndex) is to keep track of
// all newly inserted entries, and the other (localDeletionIndex) is to keep track of newly deleted
// entries (not available in localInsertionIndex). We assume that in a transaction, the insertions
// and deletions are very small, thus they can be kept in memory.
template<typename T>
class HashIndexLocalStorage {
public:
    explicit HashIndexLocalStorage(OverflowFileHandle* handle)
        : localDeletions{}, localInsertions{handle} {}
    using OwnedKeyType =
        typename std::conditional<std::same_as<T, common::ku_string_t>, std::string, T>::type;
    using Key =
        typename std::conditional<std::same_as<T, common::ku_string_t>, std::string_view, T>::type;
    HashIndexLocalLookupState lookup(Key key, common::offset_t& result) {
        if (localDeletions.contains(key)) {
            return HashIndexLocalLookupState::KEY_DELETED;
        }
        if (localInsertions.lookup(key, result)) {
            return HashIndexLocalLookupState::KEY_FOUND;
        }
        return HashIndexLocalLookupState::KEY_NOT_EXIST;
    }

    void deleteKey(Key key) {
        if (!localInsertions.deleteKey(key)) {
            localDeletions.insert(static_cast<OwnedKeyType>(key));
        }
    }

    bool insert(Key key, common::offset_t value) {
        auto iter = localDeletions.find(key);
        if (iter != localDeletions.end()) {
            localDeletions.erase(iter);
        }
        return localInsertions.append(key, value);
    }

    size_t append(const IndexBuffer<OwnedKeyType>& buffer) {
        return localInsertions.append(buffer);
    }

    bool hasUpdates() const { return !(localInsertions.empty() && localDeletions.empty()); }

    int64_t getNetInserts() const {
        return static_cast<int64_t>(localInsertions.size()) - localDeletions.size();
    }

    void clear() {
        localInsertions.clear();
        localDeletions.clear();
    }

    void applyLocalChanges(const std::function<void(Key)>& deleteOp,
        const std::function<void(const InMemHashIndex<T>&)>& insertOp) {
        for (auto& key : localDeletions) {
            deleteOp(key);
        }
        insertOp(localInsertions);
    }

    void reserveInserts(uint64_t newEntries) { localInsertions.reserve(newEntries); }

    const InMemHashIndex<T>& getInsertions() { return localInsertions; }

private:
    // When the storage type is string, allow the key type to be string_view with a custom hash
    // function
    using hash_function = typename std::conditional<std::is_same<OwnedKeyType, std::string>::value,
        common::StringUtils::string_hash, std::hash<T>>::type;
    std::unordered_set<OwnedKeyType, hash_function, std::equal_to<>> localDeletions;
    InMemHashIndex<T> localInsertions;
};

// class LocalHashIndex {
// public:
//     explicit LocalHashIndex(common::PhysicalTypeID keyDataTypeID) : keyDataTypeID{keyDataTypeID}
//     {}
//
//     bool insert(common::ValueVector& keyVector, common::offset_t startNodeOffset) {
//         bool result = false;
//         common::TypeUtils::visit(
//             keyDataTypeID,
//             [&]<IndexHashable T>(T) {
//                 localIndex;
//                 T key = keyVector->getValue<T>(vectorPos);
//                 result = insert(key, value);
//             },
//             [](auto) { KU_UNREACHABLE; });
//         return result;
//     }
//
//     template<common::IndexHashable T>
//     inline bool insert(T key, common::offset_t value) {
//         KU_ASSERT(keyDataTypeID == common::TypeUtils::getPhysicalTypeIDForType<T>());
//     }
//
//     void lookup(common::ValueVector& keyVector, common::ValueVector& nodeOffsetVector);
//
// private:
//     common::PhysicalTypeID keyDataTypeID;
//     std::unique_ptr<BaseHashIndexLocalStorage> localIndex;
// };

} // namespace storage
} // namespace kuzu
