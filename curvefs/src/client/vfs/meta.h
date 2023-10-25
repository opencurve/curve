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
 * Created Date: 2023-07-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_VFS_META_H_
#define CURVEFS_SRC_CLIENT_VFS_META_H_

#include <memory>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/common/define.h"
#include "curvefs/src/client/filesystem/meta.h"
#include "curvefs/src/client/filesystem/dir_cache.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curvefs::metaserver::InodeAttr;
using ::curvefs::client::filesystem::EntryOut;
using ::curvefs::client::filesystem::AttrOut;
using ::curvefs::client::filesystem::DirEntry;
using ::curvefs::client::filesystem::TimeSpec;
using ::curvefs::client::filesystem::DirEntryList;

using Ino = uint64_t;

const Ino ROOT_INO = ::curvefs::ROOTINODEID;

struct DirStream {
    Ino ino;
    uint64_t fh;
    uint64_t offset;
};

struct Entry {
    Entry() = default;

    Ino ino;
    InodeAttr attr;
};

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VFS_META_H_
