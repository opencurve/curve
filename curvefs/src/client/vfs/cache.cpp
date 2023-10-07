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
 * Created Date: 2023-07-04
 * Author: Jingli Chen (Wine93)
 */

#include <utility>

#include "absl/strings/str_format.h"
#include "curvefs/src/client/filesystem/utils.h"
#include "curvefs/src/client/vfs/cache.h"

namespace curvefs {
namespace client {
namespace vfs {

#define RETURN_FALSE_IF_DISABLED() \
    do {                           \
        if (!enable_) {            \
            return false;          \
        }                          \
    } while (0)

using ::curvefs::client::filesystem::Now;

EntryCache::EntryCache(size_t lruSize)
    : enable_(lruSize > 0),
      lru_(std::make_shared<LRUType>(lruSize)) {}

std::string EntryCache::EntryKey(Ino parent, const std::string& name) {
    return absl::StrFormat("%lu:%s", parent, name);
}

bool EntryCache::Get(Ino parent, const std::string& name, Ino* ino) {
    RETURN_FALSE_IF_DISABLED();
    Entry value;
    auto key = EntryKey(parent, name);
    bool yes = lru_->Get(key, &value);
    if (!yes) {
        return false;
    } else if (value.expire < Now()) {
        return false;
    }
    *ino = value.ino;
    return true;
}

bool EntryCache::Put(Ino parent,
                     const std::string& name,
                     Ino ino,
                     uint64_t timeoutSec) {
    RETURN_FALSE_IF_DISABLED();
    auto key = EntryKey(parent, name);
    Entry value;
    value.ino = ino;
    value.expire = Now() + TimeSpec(timeoutSec, 0);
    lru_->Put(key, value);
    return true;
}

bool EntryCache::Delete(Ino parent, const std::string& name) {
    RETURN_FALSE_IF_DISABLED();
    auto key = EntryKey(parent, name);
    lru_->Remove(key);
    return true;
}

size_t EntryCache::Size() {
    return lru_->Size();
}

AttrCache::AttrCache(size_t lruSize)
    : enable_(lruSize > 0),
      lru_(std::make_shared<LRUType>(lruSize)) {}

bool AttrCache::Get(Ino ino, InodeAttr* attr) {
    RETURN_FALSE_IF_DISABLED();
    Attr value;
    bool yes = lru_->Get(ino, &value);
    if (!yes) {
        return false;
    } else if (value.expire < Now()) {
        return false;
    }
    *attr = value.attr;
    return true;
}

bool AttrCache::Put(Ino ino, const InodeAttr& attr, uint64_t timeoutSec) {
    RETURN_FALSE_IF_DISABLED();
    Attr value;
    value.attr = std::move(attr);
    value.expire = Now() + TimeSpec(timeoutSec, 0);
    lru_->Put(ino, value);
    return true;
}

bool AttrCache::Delete(Ino ino) {
    RETURN_FALSE_IF_DISABLED();
    lru_->Remove(ino);
    return true;
}

size_t AttrCache::Size() {
    return lru_->Size();
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
