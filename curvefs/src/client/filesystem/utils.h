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
 * Created Date: 2023-03-06
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_UTILS_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_UTILS_H_

#include "curvefs/src/client/filesystem/core.h"

namespace curvefs {
namespace client {
namespace filesystem {

class HandlerManager {
 public:
    HandlerManager();

    std::shared_ptr<FileHandler> NewHandler();

    std::shared_ptr<FileHandler> FindHandler(uint64_t id);

    void ReleaseHandler(uint64_t id);

 private:
    Mutex mutex_;
    std::shared_ptr<DirBuffer> dirBuffer_;
    std::map<uint64_t, std::shared_ptr<FileHandler>> handlers_;
};

// directory
bool IsDir(const InodeAttr& attr);

// file which data is stored in s3
bool IsS3File(const InodeAttr& attr);

// file which data is stored in volume
bool IsVolmeFile(const InodeAttr& attr);

// symbol link
bool IsSymLink(const InodeAttr& attr);

struct TimeSpec AttrMtime(const InodeAttr& attr);

struct TimeSpec InodeMtime(const std::shared_ptr<InodeWrapper> inode);

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_UTILS_H_
