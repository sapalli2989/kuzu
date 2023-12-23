#pragma once

#include "common/file_system/file_system.h"
#include "httplib.h"

namespace kuzu {
namespace httpfs {

using HeaderMap = std::unordered_map<std::string, std::string>;

struct HTTPResponse {
public:
    HTTPResponse(httplib::Response& res, const std::string& url);
    int code;
    std::string error;
    HeaderMap headers;
    std::string url;
    std::string body;
};

struct HTTPParams {
    // TODO(Ziyi): Make them configurable.
    static constexpr uint64_t DEFAULT_TIMEOUT = 30000; // 30 sec
    static constexpr uint64_t DEFAULT_RETRIES = 3;
    static constexpr uint64_t DEFAULT_RETRY_WAIT_MS = 100;
    static constexpr float DEFAULT_RETRY_BACKOFF = 4;
    static constexpr bool DEFAULT_KEEP_ALIVE = true;
};

class HTTPFileInfo : public common::FileInfo {
public:
    HTTPFileInfo(std::string path, common::FileSystem* fileSystem, int flags);

    // We keep a http client stored for connection reuse with keep-alive headers
    std::unique_ptr<httplib::Client> httpClient;

    // File handle info
    int flags;
    uint64_t length;
    time_t last_modified;

    // Read info
    uint64_t buffer_available;
    uint64_t buffer_idx;
    uint64_t file_offset;
    uint64_t buffer_start;
    uint64_t buffer_end;

    // Read buffer
    std::unique_ptr<uint8_t[]> readBuffer;
    constexpr static uint64_t READ_BUFFER_LEN = 1000000;

protected:
    void initializeClient();
};

class HTTPFileSystem final : public common::FileSystem {
    friend class HTTPFileInfo;

public:
    std::unique_ptr<common::FileInfo> openFile(
        const std::string& path, int flags, common::FileLockType lock_type) override;

    std::vector<std::string> glob(const std::string& path) override;

    void overwriteFile(const std::string& from, const std::string& to) override { KU_UNREACHABLE; }

    void createDir(const std::string& dir) override { KU_UNREACHABLE; }

    void removeFileIfExists(const std::string& path) override { KU_UNREACHABLE; }

    bool fileOrPathExists(const std::string& path) override { KU_UNREACHABLE; }

    std::string joinPath(const std::string& base, const std::string& part) override {
        KU_UNREACHABLE;
    }

protected:
    void readFromFile(
        common::FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position) override;

    int64_t readFile(common::FileInfo* fileInfo, void* buf, size_t numBytes) override;

    void writeFile(common::FileInfo* fileInfo, const uint8_t* buffer, uint64_t numBytes,
        uint64_t offset) override {
        KU_UNREACHABLE;
    }

    int64_t seek(common::FileInfo* fileInfo, uint64_t offset, int whence) override;

    void truncate(common::FileInfo* fileInfo, uint64_t size) override { KU_UNREACHABLE; }

    uint64_t getFileSize(common::FileInfo* fileInfo) override;

    bool canHandleFile(const std::string& path) override;

private:
    static std::unique_ptr<httplib::Client> getClient(const std::string& host);

    static void parseUrl(const std::string& url, std::string& hostPath, std::string& host);

    static std::unique_ptr<httplib::Headers> getHttpHeaders(HeaderMap& headerMap);

    static std::unique_ptr<HTTPResponse> runRequestWithRetry(
        const std::function<httplib::Result(void)>& request, const std::string& url,
        std::string method, const std::function<void(void)>& retry = {});

    std::unique_ptr<HTTPResponse> headRequest(
        common::FileInfo* fileInfo, std::string url, HeaderMap headerMap);

    std::unique_ptr<HTTPResponse> getRangeRequest(common::FileInfo* fileInfo,
        const std::string& dataLen, HeaderMap headerMap, uint64_t fileOffset, char* buffer,
        uint64_t bufferLen);
};

} // namespace httpfs
} // namespace kuzu
