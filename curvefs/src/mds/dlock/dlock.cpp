/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2022-05-10
 * Author: Jingli Chen (Wine93)
 */

#include <bthread/bthread.h>

#include <memory>
#include <thread>
#include <ostream>
#include <functional>

#include "src/common/encode.h"
#include "src/common/timeutility.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/mds/dlock/dlock.h"
#include "curvefs/src/mds/common/storage_key.h"

namespace curvefs {
namespace mds {
namespace dlock {

using ::curve::common::TimeUtility;
using ::curve::common::NameLockGuard;
using ::curve::common::EncodeBigEndian;
using ::curvefs::mds::DLockValue;
using ::curvefs::mds::DLOCK_KEY_PREFIX;
using ::curvefs::mds::DLOCK_PREFIX_LENGTH;

std::ostream& operator<<(std::ostream& os,  OWNER_STATUS status) {
    switch (status) {
        case OWNER_STATUS::OK:
            os << "OK";
            break;
        case OWNER_STATUS::ERROR:
            os << "ERROR";
            break;
        case OWNER_STATUS::NOT_EXIST:
            os << "NOT_EXIST";
            break;
        case OWNER_STATUS::EXPIRED:
            os << "EXPIRED";
            break;
        default:
            os << "UNKNOWN";
            break;
    }
    return os;
}

DLock::DLock(const DLockOptions& options,
             std::shared_ptr<EtcdClientImp> etcdClient)
    : options_(options),
      etcdClient_(etcdClient) {}

LOCK_STATUS DLock::Lock(const std::string& name,
                        const std::string& owner) {
    bool succ = false;
    uint64_t total = options_.tryTimeoutMs;
    uint64_t step = options_.tryIntervalMs;
    std::string key = EncodeKey(name);
    LOG(INFO) << "DLock try lock, name=" << name << ", owner=" << owner
              << ", total=" << total << ", step=" << step << ", key=" << key;
    for (uint64_t elapsed = 0; elapsed < total; elapsed += step) {
        {
            NameLockGuard lg(nameLock_, name);
            if (ReplaceOwner(name, owner, options_.ttlMs)) {
                succ = true;
                break;
            }
        }
        bthread_usleep(step * 1000);
    }

    if (!succ) {
        LOG(ERROR) << "DLock lock failed, name=" << name
                   << ", owner=" << owner;
        return LOCK_STATUS::TIMEOUT;
    }
    LOG(INFO) << "DLock lock success, name=" << name << ", owner=" << owner;
    return LOCK_STATUS::OK;
}

LOCK_STATUS DLock::UnLock(const std::string& name,
                          const std::string& owner) {
    NameLockGuard lg(nameLock_, name);
    std::string oldOwner;
    OWNER_STATUS status = GetOwner(name, &oldOwner);
    if (status == OWNER_STATUS::EXPIRED ||
        status == OWNER_STATUS::NOT_EXIST ||
        (status == OWNER_STATUS::OK && oldOwner == owner)) {
        return ClearOwner(name) ? LOCK_STATUS::OK :
                                  LOCK_STATUS::FAILED;
    }
    return LOCK_STATUS::FAILED;
}

bool DLock::CheckOwner(const std::string& name,
                       const std::string& owner) {
    NameLockGuard lg(nameLock_, name);
    std::string oldOwner;
    OWNER_STATUS status = GetOwner(name, &oldOwner);
    LOG(INFO) << "DLock check owner, retCode=" << status
              << ", name=" << name << ", expect owner=" << owner
              << ", actual owner=" << oldOwner;
    return status == OWNER_STATUS::OK && oldOwner == owner;
}

size_t DLock::Hash(const std::string& key) {
    return std::hash<std::string>{}(key);
}

std::string DLock::EncodeKey(const std::string& name) {
    std::string key = DLOCK_KEY_PREFIX;
    size_t prefixLen = DLOCK_PREFIX_LENGTH;
    uint64_t num = static_cast<uint64_t>(Hash(name));
    key.resize(prefixLen + sizeof(uint64_t));
    EncodeBigEndian(&(key[prefixLen]), num);
    return key;
}

OWNER_STATUS DLock::GetOwner(const std::string& name,
                             std::string* owner) {
    std::string skey = EncodeKey(name);
    std::string svalue;
    DLockValue value;
    int rc = etcdClient_->Get(skey, &svalue);
    if (rc == EtcdErrCode::EtcdKeyNotExist) {
        return OWNER_STATUS::NOT_EXIST;
    } else if (rc != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Get dlock value from etcd failed"
                   << ", retCode=" << rc;
        return OWNER_STATUS::ERROR;
    } else if (!value.ParseFromString(svalue)) {
        LOG(ERROR) << "Parase dlock value from string failed";
        return OWNER_STATUS::ERROR;
    }

    uint64_t now = TimeUtility::GetTimeofDayMs();
    if (now >= value.expiretime()) {
        LOG(WARNING) << "DLock lock expired, expireTime=" << value.expiretime()
                     << ", now=" << now;
        return OWNER_STATUS::EXPIRED;
    }
    *owner = value.owner();
    return OWNER_STATUS::OK;
}

bool DLock::SetOwner(const std::string& name,
                     const std::string& owner,
                     uint64_t ttlMs) {
    std::string skey = EncodeKey(name);
    std::string svalue;
    DLockValue value;
    value.set_owner(owner);
    value.set_expiretime(TimeUtility::GetTimeofDayMs() + ttlMs);
    if (!value.SerializeToString(&svalue)) {
        LOG(ERROR) << "Serialize dlock value to string failed";
        return false;
    }

    int rc = etcdClient_->Put(skey, svalue);
    if (rc != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put dlock value to etcd failed, retCode=" << rc;
        return false;
    }
    return true;
}

bool DLock::ClearOwner(const std::string& name) {
    std::string skey = EncodeKey(name);
    int rc = etcdClient_->Delete(skey);
    if (rc != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Delete dlock value from etcd failed"
                    << ", retCode=" << rc;
        return false;
    }
    return true;
}

bool DLock::ReplaceOwner(const std::string& name,
                         const std::string& owner,
                         uint64_t ttlMs) {
    std::string oldOwner;
    OWNER_STATUS status = GetOwner(name, &oldOwner);
    LOG(INFO) << "DLock get owner, retCode=" << status
              << ", name=" << name << ", owner=" << oldOwner;
    if (status == OWNER_STATUS::NOT_EXIST ||
        status == OWNER_STATUS::EXPIRED ||
        oldOwner == owner) {
        return SetOwner(name, owner, ttlMs);
    }
    return false;
}

}  // namespace dlock
}  // namespace mds
}  // namespace curvefs
