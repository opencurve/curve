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
 * Created Date: 2023-03-12
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_CORE_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_CORE_H_

#include <sys/stat.h>
#include <glog/logging.h>

#include <map>
#include <queue>
#include <atomic>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>

#include "src/common/lru_cache.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/common/define.h"
#include "curvefs/src/client/fuse_client.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/fuse_common.h"
#include "curvefs/src/client/dir_buffer.h"
#include "curvefs/src/client/dentry_cache_manager.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/filesystem/filesystem.h"
#include "curvefs/src/client/filesystem/dir_cache.h"
#include "curvefs/src/client/filesystem/openfile.h"
#include "curvefs/src/client/filesystem/attr_watcher.h"
#include "curvefs/src/client/filesystem/rpc_client.h"
#include "curvefs/src/client/filesystem/message_queue.h"
#include "curvefs/src/client/filesystem/utils.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::curve::common::Atomic;
using ::curve::common::Mutex;
using ::curve::common::RWLock;
using ::curve::common::UniqueLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curve::common::LRUCache;
using ::curvefs::client::InodeWrapper;
using ::curvefs::client::InodeCacheManager;
using ::curvefs::client::DentryCacheManager;
using ::curvefs::client::common::FileSystemOption;
using ::curvefs::client::common::KernelCacheOption;
using ::curvefs::client::common::DirCacheOption;
using ::curvefs::client::common::OpenFileOption;
using ::curvefs::client::common::AttrWatcherOption;
using ::curvefs::client::common::RPCOption;
using ::curvefs::metaserver::XAttr;
using ::curvefs::metaserver::Dentry;
using ::curvefs::metaserver::InodeAttr;
using ::curvefs::metaserver::FsFileType;

typedef fuse_ino_t Ino;

struct TimeSpec {
    uint64_t seconds;
    uint32_t nanoSeconds;

    TimeSpec() : seconds(0), nanoSeconds(0) {}

    TimeSpec(uint64_t seconds, uint32_t nanoSeconds)
        : seconds(seconds), nanoSeconds(nanoSeconds) {}

    TimeSpec(const TimeSpec& rhs)
        : seconds(rhs.seconds), nanoSeconds(rhs.nanoSeconds) {}
};

struct Context {
    Context(fuse_req_t req) : req(req) {}

    Context(fuse_req_t req, fuse_file_info* fi)
        : req(req), fi(fi) {}

    fuse_req_t req;
    struct fuse_file_info* fi;
};

struct EntryOut {
    EntryOut(InodeAttr attr) : attr(attr) {}

    InodeAttr attr;
    double entryTimeout;
    double attrTimeout;
};

struct AttrOut {
    AttrOut(InodeAttr attr) : attr(attr) {}

    InodeAttr attr;
    double attrTimeout;
};

struct DirEntry {
    Ino ino;
    std::string name;
    InodeAttr attr;
};

struct FileHandler {
    uint64_t id;
    TimeSpec mtime;
    DirBufferHead* buffer;
};

inline bool operator==(const TimeSpec& lhs, const TimeSpec& rhs) {
    return (lhs.seconds == rhs.seconds) &&
        (lhs.nanoSeconds == rhs.nanoSeconds);
}

inline bool operator!=(const TimeSpec& lhs, const TimeSpec& rhs) {
    return !(lhs == rhs);
}

inline bool operator<(const TimeSpec& lhs, const TimeSpec& rhs) {
    return (lhs.seconds < rhs.seconds) ||
        (lhs.seconds == rhs.seconds && lhs.nanoSeconds < rhs.nanoSeconds);
}

inline bool operator>(const TimeSpec& lhs, const TimeSpec& rhs) {
    return (lhs.seconds > rhs.seconds) ||
        (lhs.seconds == rhs.seconds && lhs.nanoSeconds > rhs.nanoSeconds);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_CORE_H_
