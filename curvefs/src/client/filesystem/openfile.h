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
 * Created Date: 2023-03-09
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_OPENFILE_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_OPENFILE_H_

#include "curvefs/src/client/filesystem/core.h"

namespace curvefs {
namespace client {
namespace filesystem {

struct OpenFile {
    OpenFile(std::shared_ptr<InodeWrapper> inode)
        : inode(inode), refs(0) {}

    std::shared_ptr<InodeWrapper> inode;
    uint64_t refs;
};

class OpenFiles {
 public:
    using LRUType = LRUCache<Ino, std::shared_ptr<OpenFile>>;
    using MessageType = std::shared_ptr<OpenFile>;
    using MessageQueueType = MessageQueue<MessageType>;

 public:
    explicit OpenFiles(OpenFileOption option);

    void Open(Ino ino, std::shared_ptr<InodeWrapper> inode);

    void Close(Ino ino);

    bool IsOpened(Ino ino, std::shared_ptr<InodeWrapper>* inode);

    bool GetLength(Ino ino, uint64_t* length);

 private:
    void Evit();

    void Delete(Ino ino, const std::shared_ptr<OpenFile>& file);

    void Sync(const std::shared_ptr<OpenFile>& file);

 private:
    RWLock rwlock_;
    size_t lruSize_;
    std::shared_ptr<LRUType> files_;
    std::shared_ptr<MessageQueueType> mq_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_OPENFILE_H_
