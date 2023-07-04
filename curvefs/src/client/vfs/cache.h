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

#ifndef CURVEFS_SRC_CLIENT_VFS_CACHE_H_
#define CURVEFS_SRC_CLIENT_VFS_CACHE_H_

#include <string>
#include <memory>

#include "src/common/lru_cache.h"
#include "curvefs/src/client/vfs/meta.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curve::common::LRUCache;

class EntryCache {
 public:
    struct Entry {
        Ino ino;
        TimeSpec expire;
    };

    // FIXME: is there a more effective type for entry cache? maybe
    // absl::btree<Ino, absl::btree<std::string, Entry>> is a choise,
    // but it is a bit complex to implement lru evit strategy.
    using LRUType = LRUCache<std::string, Entry>;

 public:
    explicit EntryCache(size_t lruSize);

    bool Get(Ino parent, const std::string& name, Ino* ino);

    bool Put(Ino parent, const std::string& name, Ino ino, uint64_t timeoutSec);

 private:
    std::string EntryKey(Ino parent, const std::string& name);

 private:
    bool enable_;
    std::shared_ptr<LRUType> lru_;
};

class AttrCache {
 public:
    struct Attr {
        InodeAttr attr;
        TimeSpec expire;
    };

    using LRUType = LRUCache<Ino, Attr>;

 public:
    explicit AttrCache(size_t lruSize);

    bool Get(Ino ino, InodeAttr* attr);

    bool Put(Ino ino, const InodeAttr& attr, uint64_t timeoutSec);

 private:
    bool enable_;
    std::shared_ptr<LRUType> lru_;
};

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VFS_CACHE_H_
