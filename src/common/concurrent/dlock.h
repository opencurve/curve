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
 * Created Date: April 01 2021
 * Author: wanghai01
 */

#ifndef SRC_COMMON_CONCURRENT_DLOCK_H_
#define SRC_COMMON_CONCURRENT_DLOCK_H_

#include <string>
#include <memory>
#include "src/kvstorageclient/etcd_client.h"
#include "src/common/uncopyable.h"


namespace curve {
namespace common {

using curve::kvstorage::KVStorageClient;
using curve::common::Uncopyable;

struct DLockOpts {
    std::string pfx;
    int retryTimes;
    // the interface timeout，unit is millisecond
    int ctx_timeoutMS;
    // the session and lease timeout, unit is second
    int ttlSec;
};


class DLock : public Uncopyable {
 public:
    explicit DLock(const DLockOpts &opts) : opts_(opts), locker_(0) {}
    virtual ~DLock();

    /**
     * @brief Init the etcd Mutex
     * 
     * @return lock leaseid
     */
    virtual int64_t Init();

    /**
     * @brief lock the object
     * 
     * @return error code EtcdErrCode
     */
    virtual int Lock();

    /**
     * // TODO(wanghai01): if etcd used updated to v3.5, the TryLock can be used
     *
     * EtcdMutexTryLock (not support at etcd v3.4)
     *
     * @brief try to lock the object
     *
     * @return error code EtcdErrCode
     */
    // int TryLock();

    /**
     * @brief unlock the object
     *
     * @return error code EtcdErrCode
     */
    virtual int Unlock();

    /**
     * @brief get lock key
     *
     * @return lock key
     */
    virtual std::string GetPrefix();

 private:
    bool NeedRetry(int errCode);

 private:
    int64_t locker_;
    const DLockOpts &opts_;
};

class DLockGuard : public Uncopyable {
 public:
    explicit DLockGuard(std::shared_ptr<DLock> lock): lock_(lock) {
        lock_->Lock();
    }

    ~DLockGuard() {
        if (!release_) {
            lock_->Unlock();
        }
    }

    void Release() {
        release_ = true;
    }

 private:
    std::shared_ptr<DLock> lock_;
    bool release_;
};


}   // namespace common
}   // namespace curve


#endif  // SRC_COMMON_CONCURRENT_DLOCK_H_
