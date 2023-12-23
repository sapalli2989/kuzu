#include "processor/operator/load.h"

#include "common/exception/io.h"

#ifdef _WIN32

#include "windows.h"
#define RTLD_NOW 0
#define RTLD_LOCAL 0

inline void* dlopen(const char* file, int mode) {
    D_ASSERT(file);
    auto fpath = WindowsUtil::UTF8ToUnicode(file);
    return (void*)LoadLibraryW(fpath.c_str());
}

inline void* dlsym(void* handle, const char* name) {
    D_ASSERT(handle);
    return (void*)GetProcAddress((HINSTANCE)handle, name);
}

#else
#include <dlfcn.h>
#endif

#include "common/system_message.h"
#include "main/database.h"

using namespace kuzu::common;

typedef void (*ext_init_func_t)(kuzu::main::Database&);

namespace kuzu {
namespace processor {

bool Load::getNextTuplesInternal(ExecutionContext* context) {
    if (hasExecuted) {
        return false;
    }
    hasExecuted = true;
#ifdef _WIN32
    auto fpath = WindowsUtil::UTF8ToUnicode(path);
    auto libHdl = LoadLibraryW(fpath.c_str());
#else
    auto libHdl = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
#endif
    if (libHdl == nullptr) {
        throw common::IOException(
            stringFormat("Extension \"{}\" could not be loaded.\nError: {}", path, getDLError()));
    }
    auto load = (ext_init_func_t)(dlsym(libHdl, "init"));
    if (load == nullptr) {
        throw common::IOException(
            stringFormat("Extension \"{}\" does not have a valid init function.\nError: {}", path,
                getDLError()));
    }
    (*load)(*context->database);
    return true;
}

} // namespace processor
} // namespace kuzu
