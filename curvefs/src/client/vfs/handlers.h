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

#include "src/common/concurrent/concurrent.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curve::common::Mutex;
using ::curve::common::LockGuard;

struct FileHandler {
    Ino ino;
    size_t offset;
};

class FileHandlers {
 public:
    FileHandlers();

    int64_t NextHandler();

    bool GetHandler(int fd, std::shared_ptr<FileHandler>* handler);

    void FreeHandler(int fd);

 private:
    Mutex mutex_;
    uint64_t nextHandler_;
    std::map<int64_t, Handler> Handlers_;
};

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VFS_HANDLERS_H_
