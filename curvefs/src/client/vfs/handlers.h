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

#ifndef CURVEFS_SRC_CLIENT_VFS_HANDLERS_H_
#define CURVEFS_SRC_CLIENT_VFS_HANDLERS_H_

#include <cstdint>
#include <map>
#include <memory>

#include "absl/container/btree_map.h"
#include "curvefs/src/client/vfs/meta.h"
#include "src/common/concurrent/concurrent.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curve::common::Mutex;
using ::curve::common::LockGuard;

struct FileHandler {
    FileHandler() : ino(0), offset(0) {}
    FileHandler(Ino ino, uint64_t offset, uint64_t length, uint32_t flags)
        : ino(ino), offset(offset), length(length), flags(flags) {}

    Ino ino;
    uint64_t offset;
    uint64_t length;
    uint32_t flags;
};

class FileHandlers {
 public:
    FileHandlers();

    uint64_t NextHandler(Ino ino,
                         uint64_t offset,
                         uint64_t length,
                         uint32_t flags);

    bool GetHandler(uint64_t fd, std::shared_ptr<FileHandler>* handler);

    void FreeHandler(uint64_t fd);

 private:
    Mutex mutex_;
    uint64_t nextHandler_;
    absl::btree_map<uint64_t, std::shared_ptr<FileHandler>> handlers_;
};

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VFS_HANDLERS_H_
