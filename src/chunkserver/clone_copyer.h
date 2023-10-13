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

/*
 * Project: curve
 * Created Date: Monday March 11th 2019
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_CLONE_COPYER_H_
#define SRC_CHUNKSERVER_CLONE_COPYER_H_

#include <glog/logging.h>

#include <list>
#include <memory>
#include <string>
#include <unordered_map>

#include "include/chunkserver/chunkserver_common.h"
#include "include/client/libcurve.h"
#include "src/client/client_common.h"
#include "src/client/config_info.h"
#include "src/client/libcurve_file.h"
#include "src/common/location_operator.h"
#include "src/common/s3_adapter.h"

namespace curve {
namespace chunkserver {

using curve::client::FileClient;
using curve::client::UserInfo;
using curve::common::GetObjectAsyncCallBack;
using curve::common::GetObjectAsyncContext;
using curve::common::LocationOperator;
using curve::common::OriginType;
using curve::common::S3Adapter;
using std::string;

class DownloadClosure;

struct CopyerOptions {
    // Root user information on curvefs
    UserInfo curveUser;
    // Profile path for curvefs
    std::string curveConf;
    // Configuration file path for s3adapter
    std::string s3Conf;
    // Object pointer to curve client
    std::shared_ptr<FileClient> curveClient;
    // Object pointer to s3 adapter
    std::shared_ptr<S3Adapter> s3Client;
    // curve file's time to live
    uint64_t curveFileTimeoutSec;
};

struct AsyncDownloadContext {
    // Location information of the source chunk
    string location;
    // Request to download the relative offset of data in the object
    off_t offset;
    // The length of the requested download data
    size_t size;
    // Buffer for storing downloaded data
    char* buf;
};

struct CurveOpenTimestamp {
    // Opened file id
    int fd;
    // Opened file name
    string fileName;
    // lastest use time, using seconds
    int64_t lastUsedSec;
    // Init functions
    CurveOpenTimestamp() : fd(-1), fileName(""), lastUsedSec(0) {}
    CurveOpenTimestamp(int _fd, string _file, uint64_t _lastUsedSec)
        : fd(_fd), fileName(_file), lastUsedSec(_lastUsedSec) {}
};

std::ostream& operator<<(std::ostream& out, const AsyncDownloadContext& rhs);

class OriginCopyer {
 public:
    OriginCopyer();
    virtual ~OriginCopyer() = default;

    /**
     * Initialize Resources
     * @param options: Configuration information
     * @return: Success returns 0, failure returns -1
     */
    virtual int Init(const CopyerOptions& options);

    /**
     * Release resources
     * @return: Success returns 0, failure returns -1
     */
    virtual int Fini();

    /**
     * Asynchronous copying of data from the source
     * @param done: Contains contextual information for download requests,
     *After the data download is completed, execute the closure for callback
     */
    virtual void DownloadAsync(DownloadClosure* done);

 private:
    void DownloadFromS3(const string& objectName, off_t off, size_t size,
                        char* buf, DownloadClosure* done);
    void DownloadFromCurve(const string& fileName, off_t off, size_t size,
                           char* buf, DownloadClosure* done);
    static void DeleteExpiredCurveCache(void* arg);

 private:
    // Root user information on curvefs
    UserInfo curveUser_;
    // mutex for protect curveOpenTime_
    std::mutex timeMtx_;
    // List to maintain curve file open timestamp
    std::list<CurveOpenTimestamp> curveOpenTime_;
    // curve file's time to live
    uint64_t curveFileTimeoutSec_;
    // Responsible for interacting with curve
    std::shared_ptr<FileClient> curveClient_;
    // Responsible for interacting with s3
    std::shared_ptr<S3Adapter> s3Client_;
    // Protect fdMap_ Mutex lock for
    std::mutex mtx_;
    // File name ->Mapping of file fd
    std::unordered_map<std::string, int> fdMap_;
    // Timer for clean expired curve file
    bthread::TimerThread timer_;
    // timer's task id
    bthread::TimerThread::TaskId timerId_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CLONE_COPYER_H_
