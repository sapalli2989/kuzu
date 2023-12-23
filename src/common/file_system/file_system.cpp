#include "common/file_system/file_system.h"

#include "common/exception/io.h"

namespace kuzu {
namespace common {

void FileSystem::overwriteFile(const std::string& /*from*/, const std::string& /*to*/) {
    throw IOException{"Unsupported file operation: overwriteFile."};
}

void FileSystem::createDir(const std::string& /*dir*/) {
    throw IOException{"Unsupported file operation: createDir."};
}

void FileSystem::removeFileIfExists(const std::string& /*path*/) {
    throw IOException{"Unsupported file operation: removeFileIfExists."};
}

bool FileSystem::fileOrPathExists(const std::string& /*path*/) {
    throw IOException{"Unsupported file operation: fileOrPathExists."};
}

std::string FileSystem::joinPath(const std::string& base, const std::string& part) {
    return (std::filesystem::path{base} / part).string();
}

std::string FileSystem::getFileExtension(const std::filesystem::path& path) {
    return path.extension().string();
}

void FileSystem::writeFile(
    FileInfo* /*fileInfo*/, const uint8_t* /*buffer*/, uint64_t /*numBytes*/, uint64_t /*offset*/) {
    throw IOException{"Unsupported file operation: writeFile."};
}

void FileSystem::truncate(FileInfo* /*fileInfo*/, uint64_t /*size*/) {
    throw IOException{"Unsupported file operation: truncate."};
}

} // namespace common
} // namespace kuzu
