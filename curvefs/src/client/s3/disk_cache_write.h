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

#include <memory>
#include <string>
#include <list>
#include <set>

#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "src/common/lru_cache.h"
#include "src/common/throttle.h"
#include "src/common/wait_interval.h"
#include "curvefs/src/common/wrap_posix.h"
#include "curvefs/src/common/utils.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "curvefs/src/client/s3/disk_cache_write.h"
#include "curvefs/src/client/s3/disk_cache_read.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/s3/disk_cache_base.h"

namespace curvefs {
namespace client {

using curvefs::common::PosixWrapper;
using curve::common::InterruptibleSleeper;
using ::curve::common::SglLRUCache;
using curve::common::PutObjectAsyncCallBack;

class DiskCacheWrite : public DiskCacheBase {
 public:
    class SynchronizationTask {
     public:
        explicit SynchronizationTask(int enventNum) {
            countDownEnvent_.Reset(enventNum);
            errorCount_ = 0;
        }
        void Wait() { countDownEnvent_.Wait(); }

        void Signal() { countDownEnvent_.Signal(); }

        void SetError() { errorCount_.fetch_add(1); }

        bool Success() { return errorCount_ == 0; }

     public:
        curve::common::CountDownEvent countDownEnvent_;
        std::atomic<int> errorCount_;
    };

 public:
    // init isRunning_ should here，
    // otherwise when call AsyncUploadStop in ~DiskCacheWrite will failed:
    // "terminate called after throwing an instance of 'std::system_error'"
    DiskCacheWrite() : isRunning_(false) {}
    virtual ~DiskCacheWrite() {
       AsyncUploadStop();
    }
    void Init(S3Client *client, std::shared_ptr<PosixWrapper> posixWrapper,
              const std::string cacheDir, uint64_t asyncLoadPeriodMs,
              std::shared_ptr<SglLRUCache<std::string>> cachedObjName);
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
     * @param[in] syncTask wait upload finish
     * @return success: 0, fail : < 0
     */
    virtual int
    UploadFile(const std::string &name,
               std::shared_ptr<SynchronizationTask> syncTask = nullptr);

    virtual int UploadFileByInode(const std::string &inode);

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

    virtual void InitMetrics(std::shared_ptr<DiskCacheMetric> metric) {
        metric_ = metric;
    }

 private:
    int AsyncUploadFunc();
    void UploadFile(const std::list<std::string> &toUpload,
                    std::shared_ptr<SynchronizationTask> syncTask = nullptr);
    bool WriteCacheValid();
    int GetUploadFile(const std::string &inode,
                      std::list<std::string> *toUpload);
    int FileExist(const std::string &inode);

    curve::common::Thread backEndThread_;
    curve::common::Atomic<bool> isRunning_;
    std::list<std::string> waitUpload_;
    bthread::Mutex mtx_;
    InterruptibleSleeper sleeper_;
    uint64_t asyncLoadPeriodMs_;
    S3Client *client_;
    // file system operation encapsulation
    std::shared_ptr<PosixWrapper> posixWrapper_;
    std::shared_ptr<DiskCacheMetric> metric_;

    std::shared_ptr<SglLRUCache<std::string>> cachedObjName_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_DISK_CACHE_WRITE_H_
