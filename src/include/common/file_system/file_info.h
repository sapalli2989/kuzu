#pragma once

#include <cstdint>
#include <string>

#include "common/api.h"

namespace kuzu {
namespace common {

class FileSystem;

struct FileInfo {
    FileInfo(std::string path, FileSystem* fileSystem)
        : path{std::move(path)}, fileSystem{fileSystem} {}

    virtual ~FileInfo() = default;

    KUZU_API uint64_t getFileSize();

    KUZU_API void readFromFile(void* buffer, uint64_t numBytes, uint64_t position);

    KUZU_API int64_t readFile(void* buf, size_t nbyte);

    KUZU_API void writeFile(const uint8_t* buffer, uint64_t numBytes, uint64_t offset);

    KUZU_API int64_t seek(uint64_t offset, int whence);

    KUZU_API void truncate(uint64_t size);

    const std::string path;

    FileSystem* fileSystem;
};

} // namespace common
} // namespace kuzu
