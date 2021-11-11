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
 * Created Date: 21-08-13
 * Author: wuhongsong
 */
#ifndef CURVEFS_SRC_CLIENT_S3_DISK_CACHE_WRITE_H_
#define CURVEFS_SRC_CLIENT_S3_DISK_CACHE_WRITE_H_

#include <sys/stat.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <string>
#include <list>
#include <set>

#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "curvefs/src/common/wrap_posix.h"
#include "curvefs/src/client/s3/disk_cache_base.h"
#include "curvefs/src/client/s3/client_s3.h"

namespace curvefs {
namespace client {

using curvefs::common::PosixWrapper;
using curve::common::InterruptibleSleeper;
using curve::common::PutObjectAsyncCallBack;

class DiskCacheWrite : public DiskCacheBase {
 public:
    // init isRunning_ should here，
    // otherwise when call AsyncUploadStop in ~DiskCacheWrite will failed:
    // "terminate called after throwing an instance of 'std::system_error'"
    DiskCacheWrite() : isRunning_(false) {}
    virtual ~DiskCacheWrite() {
       AsyncUploadStop();
    }
    void Init(S3Client *client, std::shared_ptr<PosixWrapper> posixWrapper,
              const std::string cacheDir, uint64_t asyncLoadPeriodMs);
    /**
     * @brief write obj to write cahce disk
     * @param[in] client S3Client
     * @param[in] option config option
     * @return success: 0, fail : < 0
     */
    virtual int WriteDiskFile(const std::string fileName,
                              const char* buf, uint64_t length,
                              bool force = true);
    /**
    * @brief after reboot，upload all files store in write cache to s3
    */
    virtual int UploadAllCacheWriteFile();
    /**
    * @brief remove from write cache
    */
    virtual int RemoveFile(const std::string fileName);
    virtual int ReadFile(const std::string name, char** buf,
      uint64_t* size);
    /**
     * @brief upload file in write cache to S3 
     * @param[in] name file name
     * @return success: 0, fail : < 0
     */
    virtual int UploadFile(const std::string name);
    /**
     * @brief: start aync upload thread
     */
    virtual int AsyncUploadRun();
    /**
     * @brief enqueue the file, then upload to S3
     * @param[in] objName obj name
     */
    virtual void AsyncUploadEnqueue(const std::string objName);
    /**
     * @brief: stop aync upload thread.
     */
    virtual int AsyncUploadStop();

 private:
    virtual int AsyncUploadFunc();

    curve::common::Thread backEndThread_;
    curve::common::Atomic<bool> isRunning_;
    std::list<std::string> waitUpload_;
    bthread::Mutex mtx_;
    bthread::ConditionVariable cond_;
    InterruptibleSleeper sleeper_;
    uint64_t asyncLoadPeriodMs_;
    S3Client *client_;

    // file system operation encapsulation
    std::shared_ptr<PosixWrapper> posixWrapper_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_DISK_CACHE_WRITE_H_
