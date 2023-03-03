/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 * Project: curve
 * File Created: 2020-12-23
 * Author: wanghai01
 */

#ifndef SRC_CLIENT_SOURCE_READER_H_
#define SRC_CLIENT_SOURCE_READER_H_

#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <memory>

#include "src/client/config_info.h"
#include "src/common/interruptible_sleeper.h"

namespace curve {
namespace client {

class FileInstance;
class MDSClient;
struct RequestContext;
struct UserInfo;

class SourceReader {
 public:
    class ReadHandler {
        friend class SourceReader;

     public:
        ReadHandler(FileInstance* file, time_t t, bool ownfile)
            : file_(file), lastUsedSec_(t), ownfile_(ownfile) {}
        ~ReadHandler();

        ReadHandler(const ReadHandler&) = delete;
        ReadHandler& operator=(const ReadHandler&) = delete;

     private:
        FileInstance* file_;
        std::atomic<time_t> lastUsedSec_;
        bool ownfile_;
    };

    static SourceReader& GetInstance() {
        static SourceReader reader;
        return reader;
    }

    void SetOption(const FileServiceOption& opt);

    /**
     * run the timed stop fd thread
     */
    void Run();

    /**
     * stop the timed stop fd thread
     */
    void Stop();

    /**
     *  @brief read from the source
     *  @param reqCtxVec the read request context vector
     *  @param userInfo the user info
     *  @param mdsclient interact with metadata server
     *  @return 0 success; -1 fail
     */
    int Read(const std::vector<RequestContext*>& reqCtxVec,
             const UserInfo& userInfo, MDSClient* mdsClinet);

    // for unit-test
    std::unordered_map<std::string, ReadHandler>& GetReadHandlers();

    // for unit-test
    void SetReadHandlers(
        const std::unordered_map<std::string, ReadHandler>& handlers);

 private:
    SourceReader() = default;
    ~SourceReader();
    SourceReader(const SourceReader&);
    SourceReader& operator=(const SourceReader&);

    /**
     * close the timeout fd with timed thread
     */
    void Closefd();

    ReadHandler* GetReadHandler(const std::string& fileName,
                                const UserInfo& userInfo, MDSClient* mdsclient);

 private:
    // the mutex lock for readHandlers_
    curve::common::RWLock rwLock_;

    // store file read handlers
    // key: filename, value: file read handler
    std::unordered_map<std::string, ReadHandler> readHandlers_;

    // is initialized
    bool inited_ = false;
    std::atomic<bool> running_;
    std::unique_ptr<std::thread> fdCloseThread_;
    std::unique_ptr<curve::common::InterruptibleSleeper> sleeper_;

    FileServiceOption fileOption_;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_SOURCE_READER_H_
