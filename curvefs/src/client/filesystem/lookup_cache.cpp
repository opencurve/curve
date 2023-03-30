/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-03-31
 * Author: Jingli Chen (Wine93)
 */

#include <glog/logging.h>

#include <ctime>

#include "absl/strings/str_format.h"
#include "curvefs/src/client/filesystem/utils.h"
#include "curvefs/src/client/filesystem/lookup_cache.h"

namespace curvefs {
namespace client {
namespace filesystem {

LookupCache::LookupCache(LookupCacheOption option)
    : enable_(option.negativeTimeout > 0),
      rwlock_(),
      option_(option) {
    lru_ = std::make_shared<LRUType>(option.lruSize);
}

std::string LookupCache::Entry2Key(Ino parent, const std::string& name) {
    return absl::StrFormat("%d:%s", parent, name);
}

bool LookupCache::Get(Ino parent, const std::string& name) {
    if (!enable_) {
        return false;
    }

    ReadLockGuard lk(rwlock_);
    std::string key = Entry2Key(parent, name);
    TimeSpec expireTime;
    bool yes = lru_->Get(key, &expireTime);
    if (!yes) {
        return false;
    } else if (expireTime > Now()) {
        return false;
    }
    return true;
}

void LookupCache::Put(Ino parent, const std::string& name) {
    if (!enable_) {
        return;
    }

    WriteLockGuard lk(rwlock_);
    auto key = Entry2Key(parent, name);
    TimeSpec expireTime = Now() + TimeSpec(option_.negativeTimeout, 0);
    lru_->Put(key, expireTime);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
