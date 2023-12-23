#include "httpfs.h"

#include "common/cast.h"
#include "common/exception/io.h"

namespace kuzu {
namespace httpfs {

using namespace kuzu::common;

HTTPResponse::HTTPResponse(httplib::Response& res, const std::string& url)
    : code{res.status}, error{res.reason}, url{url}, body{res.body} {
    for (auto& [name, value] : res.headers) {
        headers[name] = value;
    }
}

HTTPFileInfo::HTTPFileInfo(std::string path, FileSystem* fileSystem, int flags)
    : FileInfo{std::move(path), fileSystem}, flags{flags}, length{0}, buffer_available{0},
      buffer_idx{0}, file_offset{0}, buffer_start{0}, buffer_end{0} {
    initializeClient();
    auto hfs = ku_dynamic_cast<FileSystem*, HTTPFileSystem*>(fileSystem);
    auto res = hfs->headRequest(ku_dynamic_cast<HTTPFileInfo*, FileInfo*>(this), this->path, {});
    std::string rangeLength;

    if (res->code != 200) {
        if (((flags & O_ACCMODE) == O_WRONLY) && res->code == 404) {
            if (!(flags & O_CREAT)) {
                // LCOV_EXCL_START
                throw IOException(stringFormat("Unable to open URL \"{}\" for writing: file does "
                                               "not exist and CREATE flag is not set",
                    this->path));
                // LCOV_EXCL_STOP
            }
            length = 0;
            return;
        } else {
            // HEAD request fail, use Range request for another try (read only one byte).
            if (((flags & O_ACCMODE) == O_RDONLY) && res->code != 404) {
                auto rangeRequest =
                    hfs->getRangeRequest(this, this->path, {}, 0, nullptr /* buffer */, 2);
                if (rangeRequest->code != 206) {
                    // LCOV_EXCL_START
                    throw IOException(stringFormat("Unable to connect to URL \"{}}\": {} ({})",
                        this->path, res->code, res->error));
                    // LCOV_EXCL_STOP
                }
                auto rangeFound = rangeRequest->headers["Content-Range"].find("/");

                if (rangeFound == std::string::npos ||
                    rangeRequest->headers["Content-Range"].size() < rangeFound + 1) {
                    // LCOV_EXCL_START
                    throw IOException(stringFormat("Unknown Content-Range Header \"The value of "
                                                   "Content-Range Header\":  ({})",
                        rangeRequest->headers["Content-Range"]));
                    // LCOV_EXCL_STOP
                }

                rangeLength = rangeRequest->headers["Content-Range"].substr(rangeFound + 1);
                if (rangeLength == "*") {
                    // LCOV_EXCL_START
                    throw IOException(
                        stringFormat("Unknown total length of the document \"{}\": {} ({})",
                            this->path, res->code, res->error));
                    // LCOV_EXCL_STOP
                }
                res = std::move(rangeRequest);
            } else {
                // LCOV_EXCL_START
                throw IOException(stringFormat("Unable to connect to URL \"{}\": {} ({})", res->url,
                    std::to_string(res->code), res->error));
                // LCOV_EXCL_STOP
            }
        }
    }

    // Initialize the read buffer now that we know the file exists
    if ((flags & O_ACCMODE) == O_RDONLY) {
        readBuffer = std::make_unique<uint8_t[]>(READ_BUFFER_LEN);
    }

    if (res->headers.find("Content-Length") == res->headers.end() ||
        res->headers["Content-Length"].empty()) {
        // There was no content-length header, we can not do range requests here, so we set the
        // length to 0.
        length = 0;
    } else {
        try {
            if (res->headers.find("Content-Range") == res->headers.end() ||
                res->headers["Content-Range"].empty()) {
                length = std::stoll(res->headers["Content-Length"]);
            } else {
                length = std::stoll(rangeLength);
            }
        } catch (std::invalid_argument& e) {
            // LCOV_EXCL_START
            throw IOException(stringFormat(
                "Invalid Content-Length header received: {}", res->headers["Content-Length"]));
            // LCOV_EXCL_STOP
        } catch (std::out_of_range& e) {
            // LCOV_EXCL_START
            throw IOException(stringFormat(
                "Invalid Content-Length header received: {}", res->headers["Content-Length"]));
            // LCOV_EXCL_STOP
        }
    }

    //    if (!res->headers["Last-Modified"].empty()) {
    //        auto result = StrpTimeFormat::Parse("%a, %d %h %Y %T %Z",
    //        res->headers["Last-Modified"]);
    //
    //        struct tm tm {};
    //        tm.tm_year = result.data[0] - 1900;
    //        tm.tm_mon = result.data[1] - 1;
    //        tm.tm_mday = result.data[2];
    //        tm.tm_hour = result.data[3];
    //        tm.tm_min = result.data[4];
    //        tm.tm_sec = result.data[5];
    //        tm.tm_isdst = 0;
    //        last_modified = mktime(&tm);
    //    }
}

void HTTPFileInfo::initializeClient() {
    std::string hostPath, host;
    HTTPFileSystem::parseUrl(path, hostPath, host);
    httpClient = HTTPFileSystem::getClient(host.c_str());
}

std::unique_ptr<common::FileInfo> HTTPFileSystem::openFile(
    const std::string& path, int flags, common::FileLockType /*lock_type*/) {
    return std::make_unique<HTTPFileInfo>(path, this, flags);
}

std::vector<std::string> HTTPFileSystem::glob(const std::string& path) {
    return {path};
}

void HTTPFileSystem::readFromFile(
    common::FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position) {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    auto to_read = numBytes;
    auto buffer_offset = 0;

    if (position >= httpFileInfo->buffer_start && position < httpFileInfo->buffer_end) {
        httpFileInfo->file_offset = position;
        httpFileInfo->buffer_idx = position - httpFileInfo->buffer_start;
        httpFileInfo->buffer_available =
            (httpFileInfo->buffer_end - httpFileInfo->buffer_start) - httpFileInfo->buffer_idx;
    } else {
        // reset buffer
        httpFileInfo->buffer_available = 0;
        httpFileInfo->buffer_idx = 0;
        httpFileInfo->file_offset = position;
    }
    while (to_read > 0) {
        auto buffer_read_len = std::min<uint64_t>(httpFileInfo->buffer_available, to_read);
        if (buffer_read_len > 0) {
            KU_ASSERT(httpFileInfo->buffer_start + httpFileInfo->buffer_idx + buffer_read_len <=
                      httpFileInfo->buffer_end);
            memcpy((char*)buffer + buffer_offset,
                httpFileInfo->readBuffer.get() + httpFileInfo->buffer_idx, buffer_read_len);

            buffer_offset += buffer_read_len;
            to_read -= buffer_read_len;

            httpFileInfo->buffer_idx += buffer_read_len;
            httpFileInfo->buffer_available -= buffer_read_len;
            httpFileInfo->file_offset += buffer_read_len;
        }

        if (to_read > 0 && httpFileInfo->buffer_available == 0) {
            auto new_buffer_available = std::min<uint64_t>(
                httpFileInfo->READ_BUFFER_LEN, httpFileInfo->length - httpFileInfo->file_offset);

            // Bypass buffer if we read more than buffer size.
            if (to_read > new_buffer_available) {
                getRangeRequest(httpFileInfo, httpFileInfo->path, {}, position + buffer_offset,
                    (char*)buffer + buffer_offset, to_read);
                httpFileInfo->buffer_available = 0;
                httpFileInfo->buffer_idx = 0;
                httpFileInfo->file_offset += to_read;
                break;
            } else {
                getRangeRequest(httpFileInfo, httpFileInfo->path, {}, httpFileInfo->file_offset,
                    (char*)httpFileInfo->readBuffer.get(), new_buffer_available);
                httpFileInfo->buffer_available = new_buffer_available;
                httpFileInfo->buffer_idx = 0;
                httpFileInfo->buffer_start = httpFileInfo->file_offset;
                httpFileInfo->buffer_end = httpFileInfo->buffer_start + new_buffer_available;
            }
        }
    }
}

int64_t HTTPFileSystem::readFile(common::FileInfo* fileInfo, void* buf, size_t numBytes) {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    auto maxNumBytesToRead = httpFileInfo->length - httpFileInfo->file_offset;
    numBytes = std::min<uint64_t>(maxNumBytesToRead, numBytes);
    if (httpFileInfo->file_offset > httpFileInfo->getFileSize()) {
        return 0;
    }
    readFromFile(fileInfo, buf, numBytes, httpFileInfo->file_offset);
    return numBytes;
}

int64_t HTTPFileSystem::seek(common::FileInfo* fileInfo, uint64_t offset, int /*whence*/) {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    httpFileInfo->file_offset = offset;
    return offset;
}

uint64_t HTTPFileSystem::getFileSize(common::FileInfo* fileInfo) {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    return httpFileInfo->length;
}

bool HTTPFileSystem::canHandleFile(const std::string& path) {
    return path.rfind("https://", 0) == 0 || path.rfind("http://", 0) == 0;
}

std::unique_ptr<httplib::Client> HTTPFileSystem::getClient(const std::string& host) {
    auto client = std::make_unique<httplib::Client>(host);
    client->set_follow_location(true);
    client->set_keep_alive(HTTPParams::DEFAULT_KEEP_ALIVE);
    // TODO(Ziyi): This option is needed for https connection
    // client->enable_server_certificate_verification(false);
    client->set_write_timeout(HTTPParams::DEFAULT_TIMEOUT);
    client->set_read_timeout(HTTPParams::DEFAULT_TIMEOUT);
    client->set_connection_timeout(HTTPParams::DEFAULT_TIMEOUT);
    client->set_decompress(false);
    return client;
}

void HTTPFileSystem::parseUrl(const std::string& url, std::string& hostPath, std::string& host) {
    if (url.rfind("http://", 0) != 0 && url.rfind("https://", 0) != 0) {
        throw IOException("URL needs to start with http:// or https://");
    }
    auto hostPathPos = url.find('/', 8);
    // LCOV_EXCL_START
    if (hostPathPos == std::string::npos) {
        throw IOException("URL needs to contain a '/' after the host");
    }
    // LCOV_EXCL_STOP
    host = url.substr(0, hostPathPos);
    hostPath = url.substr(hostPathPos);
    // LCOV_EXCEL_START
    if (hostPath.empty()) {
        throw IOException("URL needs to contain a path");
    }
    // LCOV_EXCEL_STOP
}

std::unique_ptr<httplib::Headers> HTTPFileSystem::getHttpHeaders(HeaderMap& headerMap) {
    auto headers = std::make_unique<httplib::Headers>();
    for (auto& entry : headerMap) {
        headers->insert(entry);
    }
    return headers;
}

std::unique_ptr<HTTPResponse> HTTPFileSystem::runRequestWithRetry(
    const std::function<httplib::Result(void)>& request, const std::string& url, std::string method,
    const std::function<void(void)>& retry) {
    uint64_t tries = 0;
    while (true) {
        std::exception_ptr exception = nullptr;
        httplib::Error err;
        httplib::Response response;
        int status;

        try {
            auto res = request();
            err = res.error();
            if (err == httplib::Error::Success) {
                status = res->status;
                response = res.value();
            }
        } catch (IOException& e) { exception = std::current_exception(); }

        if (err == httplib::Error::Success) {
            switch (status) {
            case 408: // Request Timeout
            case 418: // Server is pretending to be a teapot
            case 429: // Rate limiter hit
            case 503: // Server has error
            case 504: // Server has error
                break;
            default:
                return std::make_unique<HTTPResponse>(response, url);
            }
        }

        tries += 1;

        if (tries <= HTTPParams::DEFAULT_RETRIES) {
            if (tries > 1) {
                auto sleepTime = (uint64_t)((float)HTTPParams::DEFAULT_RETRY_WAIT_MS *
                                            pow(HTTPParams::DEFAULT_RETRY_BACKOFF, tries - 2));
                std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
            }
            if (retry) {
                retry();
            }
        } else {
            if (exception) {
                std::rethrow_exception(exception);
            } else if (err == httplib::Error::Success) {
                // LCOV_EXCL_START
                throw IOException(stringFormat(
                    "Request returned HTTP {} for HTTP {} to '{}'", status, method, url));
                // LCOV_EXCL_STOP
            } else {
                // LCOV_EXCL_START
                throw IOException(
                    stringFormat("{} error for HTTP {} to '{}'", to_string(err), method, url));
                // LCOV_EXCL_STOP
            }
        }
    }
}

std::unique_ptr<HTTPResponse> HTTPFileSystem::headRequest(
    FileInfo* fileInfo, std::string url, HeaderMap headerMap) {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    std::string hostPath, host;
    parseUrl(url, hostPath, host);
    auto headers = getHttpHeaders(headerMap);

    std::function<httplib::Result(void)> request(
        [&]() { return httpFileInfo->httpClient->Head(hostPath.c_str(), *headers); });

    std::function<void(void)> retry([&]() { httpFileInfo->httpClient = getClient(host); });

    return runRequestWithRetry(request, url, "HEAD", retry);
}

std::unique_ptr<HTTPResponse> HTTPFileSystem::getRangeRequest(FileInfo* fileInfo,
    const std::string& url, HeaderMap headerMap, uint64_t fileOffset, char* buffer,
    uint64_t bufferLen) {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    std::string hostPath, host;
    parseUrl(url, hostPath, host);
    auto headers = getHttpHeaders(headerMap);

    headers->insert(std::make_pair(
        "Range", stringFormat("bytes={}-{}", fileOffset, fileOffset + bufferLen - 1)));

    uint64_t bufferOffset = 0;

    std::function<httplib::Result(void)> request([&]() {
        return httpFileInfo->httpClient->Get(
            hostPath.c_str(), *headers,
            [&](const httplib::Response& response) {
                if (response.status >= 400) {
                    // LCOV_EXCL_START
                    auto error =
                        stringFormat("HTTP GET error on '{}' (HTTP {})", url, response.status);
                    if (response.status == 416) {
                        error += "Try confirm the server supports range requests.";
                    }
                    throw IOException(error);
                    // LCOV_EXCL_STOP
                }
                if (response.status < 300) {
                    bufferOffset = 0;
                    if (response.has_header("Content-Length")) {
                        auto content_length = stoll(response.get_header_value("Content-Length", 0));
                        if ((uint64_t)content_length != bufferLen) {
                            // LCOV_EXCL_START
                            throw IOException(
                                "HTTP GET error: Content-Length from server mismatches requested "
                                "range, server may not support range requests.");
                            // LCOV_EXCL_STOP
                        }
                    }
                }
                return true;
            },
            [&](const char* data, size_t dataLen) {
                if (buffer != nullptr) {
                    if (dataLen + bufferOffset > bufferLen) {
                        // LCOV_EXCL_START
                        // To avoid corruption of memory, we bail out.
                        throw IOException("Server sent back more data than expected.");
                        // LCOV_EXCL_STOP
                    }
                    memcpy(buffer + bufferOffset, data, dataLen);
                    bufferOffset += dataLen;
                }
                return true;
            });
    });
    std::function<void(void)> on_retry([&]() { httpFileInfo->httpClient = getClient(host); });
    return runRequestWithRetry(request, url, "GET Range", on_retry);
}

} // namespace httpfs
} // namespace kuzu
