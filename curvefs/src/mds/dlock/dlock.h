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

#ifndef CURVEFS_SRC_MDS_DLOCK_DLOCK_H_
#define CURVEFS_SRC_MDS_DLOCK_DLOCK_H_

#include <string>
#include <memory>

#include "src/common/encode.h"
#include "src/common/uncopyable.h"
#include "src/common/concurrent/name_lock.h"
#include "src/kvstorageclient/etcd_client.h"

namespace curvefs {
namespace mds {
namespace dlock {

using ::curve::common::NameLock;
using ::curve::common::Uncopyable;
using ::curve::kvstorage::EtcdClientImp;

struct DLockOptions {
    uint64_t ttlMs = 5000;

    uint64_t tryTimeoutMs = 3000;

    uint64_t tryIntervalMs = 50;
};

enum class LOCK_STATUS {
    OK,
    FAILED,
    TIMEOUT,
};

enum class OWNER_STATUS {
    OK,
    ERROR,
    NOT_EXIST,
    EXPIRED,
};

class DLock : public Uncopyable {
 public:
    DLock(const DLockOptions& options,
          std::shared_ptr<EtcdClientImp> etcdClient);

    ~DLock() = default;

    /**
     * @brief: lock the specified namespace
     * @param[in] name: namespace to lock
     * @param[in] owner: owner for lock
     * @return return LOCK_STATUS::OK if acquire lock success,
     *         otherwise return LOCK_STATUS::TIMEOUT
     */
    LOCK_STATUS Lock(const std::string& name,
                     const std::string& owner);

    /**
     * @brief: unlock the specified namespace
     * @param[in] name: namespace to unlock
     * @param[in] owner: owner for lock
     * @return return LOCK_STATUS::OK if release lock success,
     *         otherwise return LOCK_STATUS::TIMEOUT
     */
    LOCK_STATUS UnLock(const std::string& name,
                       const std::string& owner);

    /**
     * @brief: check whether owner locked the namespace
     * @param[in] name: namespace to check
     * @param[in] owner: owner for lock
     * @return return ture if owner locked the namespace,
     *         otherwise return false
     */
    bool CheckOwner(const std::string& name,
                    const std::string& owner);

 private:
    size_t Hash(const std::string& key);

    std::string EncodeKey(const std::string& name);

    OWNER_STATUS GetOwner(const std::string& name,
                          std::string* owner);

    bool SetOwner(const std::string& name,
                  const std::string& owner,
                  uint64_t ttlMs);

    bool ClearOwner(const std::string& name);

    bool ReplaceOwner(const std::string& name,
                      const std::string& owner,
                      uint64_t ttlMs);

 private:
    NameLock nameLock_;
    DLockOptions options_;
    std::shared_ptr<EtcdClientImp> etcdClient_;
};

}  // namespace dlock
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_DLOCK_DLOCK_H_
