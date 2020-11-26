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

#include <mutex>
#include <unordered_map>
#include <thread>
#include <atomic>
#include <vector>
#include <string>
#include <utility>
#include "include/client/libcurve.h"
#include "src/client/libcurve_file.h"
#include "src/client/request_closure.h"
#include "src/common/interruptible_sleeper.h"

using curve::client::ClusterContext;
using curve::client::FileClient;
using curve::client::RequestContext;
using curve::client::UserInfo_t;

namespace curve {
namespace client {

class SourceReader {
 public:
    static SourceReader& GetInstance();

    void SetFileClient(FileClient *client) {
        fileClient_ = client;
    }

    FileClient* GetFileClient() {
        return fileClient_;
    }

    std::unordered_map<std::string,
                       std::pair<int, std::atomic<time_t>>>& GetFdMap() {
        return fdMap_;
    }

    int Init(const std::string& configPath);

    void Uinit();

    /**
     * run the timed stop fd thread
     */
    void Run();

    /**
     * stop the timed stop fd thread
     */
    void Stop();

    /**
     *  read from the origin
     *  @param: reqCtxVec the read request context vector
     *  @param: the user info
     *  @return 0 success; -1 fail
     */
    int Read(std::vector<RequestContext*> reqCtxVec,
                       const UserInfo_t& userInfo);

 private:
    SourceReader();
    ~SourceReader();
    SourceReader(const SourceReader &);
    SourceReader& operator=(const SourceReader &);

    /**
     * close the timeout fd with timed thread
     */
    int Closefd();

    /**
     * get fd
     */
    int Getfd(const std::string& fileName, const UserInfo_t& userInfo);

    FileClient *fileClient_{nullptr};
    // the mutex lock for fdMap_
    curve::common::RWLock  rwLock_;
    // first: filename; second: <fd,timestamp>
    std::unordered_map<std::string, std::pair<int, std::atomic<time_t>>> fdMap_;
    // is initialized
    bool inited_ = false;
    std::thread fdCloseThread_;
    std::atomic<bool> running_;
    curve::common::InterruptibleSleeper sleeper_;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_SOURCE_READER_H_
