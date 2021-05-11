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

#include "src/common/concurrent/dlock.h"
#include <glog/logging.h>

namespace curve {
namespace common {

int64_t DLock::Init() {
    LOG(INFO) << "DLock::Init:"
              << " pfx: " << opts_.pfx
              << ", ctx_timeoutMS: " << opts_.ctx_timeoutMS
              << ", ttlSec: " << opts_.ttlSec;
    locker_ = NewEtcdMutex(const_cast<char*>(opts_.pfx.c_str()),
                           opts_.pfx.size(), opts_.ttlSec);
    return locker_;
}

DLock::~DLock() {
    DestoryEtcdMutex(locker_);
}

int DLock::Lock() {
    bool needRetry = false;
    int retry = 0;
    int errCode;
    do {
        LOG(INFO) << "DLock::Lock locker_ = " << locker_;
        errCode = EtcdMutexLock(opts_.ctx_timeoutMS, locker_);
        needRetry = NeedRetry(errCode);
    } while (needRetry && ++retry <= opts_.retryTimes);

    if (retry > opts_.retryTimes) {
        LOG(ERROR) << "DLock::Lock timeout"
                   << ", timeoutMS = " << opts_.ctx_timeoutMS
                   << ", retryTimes = " << opts_.retryTimes;
    }
    return errCode;
}

// TODO(wanghai01): if etcd used updated to v3.5, the TryLock can be used
// int DLock::TryLock() {
//     bool needRetry = false;
//     int retry = 0;
//     int errCode;
//     do {
//         errCode = EtcdMutexTryLock(timeout_, locker_);
//         needRetry = NeedRetry(errCode);
//     } while (needRetry && ++retry <= retryTimes_);

//     return errCode;
// }

int DLock::Unlock() {
    bool needRetry = false;
    int retry = 0;
    int errCode;
    do {
        LOG(INFO) << "DLock::Unlock locker_ = " << locker_;
        errCode = EtcdMutexUnlock(opts_.ctx_timeoutMS, locker_);
        needRetry = NeedRetry(errCode);
    } while (needRetry && ++retry <= opts_.retryTimes);

    if (retry > opts_.retryTimes) {
        LOG(ERROR) << "DLock::Lock timeout"
                   << ", timeoutMS = " << opts_.ctx_timeoutMS
                   << ", retryTimes = " << opts_.retryTimes;
    }
    return errCode;
}

std::string DLock::GetPrefix() {
    return opts_.pfx;
}

bool DLock::NeedRetry(int errCode) {
    switch (errCode) {
        case EtcdErrCode::EtcdDeadlineExceeded:
        case EtcdErrCode::EtcdResourceExhausted:
        case EtcdErrCode::EtcdFailedPrecondition:
        case EtcdErrCode::EtcdAborted:
        case EtcdErrCode::EtcdUnavailable:
            return true;
    }
    return false;
}

}   // namespace common
}   // namespace curve
