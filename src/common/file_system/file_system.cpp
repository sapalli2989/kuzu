#include "common/file_system/file_system.h"

namespace kuzu {
namespace common {

std::string FileSystem::getFileExtension(const std::filesystem::path& path) {
    return path.extension().string();
}

} // namespace common
} // namespace kuzu
