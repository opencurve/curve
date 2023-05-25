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

#include "curvefs/src/client/filesystem/openfile.h"
#include "curvefs/src/client/filesystem/utils.h"

namespace curvefs {
namespace client {
namespace filesystem {

OpenFiles::OpenFiles(OpenFilesOption option,
                     std::shared_ptr<DeferSync> deferSync)
    : rwlock_(),
      option_(option),
      deferSync_(deferSync),
      files_(std::make_shared<LRUType>(option.lruSize)) {
    LOG(INFO) << "Using openfile lru cache, capacity " << option.lruSize;
}

/*
 * Delete(...) does:
 *   1) publish to message queue which will flush file to server with async
 *   2) delete file from lru cache
 */
void OpenFiles::Delete(Ino ino,
                       const std::shared_ptr<OpenFile>& file,
                       bool flush) {
    if (flush) {
        deferSync_->Push(file->inode);
    }
    files_->Remove(ino);

    VLOG(1) << "Delete open file cache: ino = " << ino
            << ", refs = " << file->refs
            << ", mtime = " << InodeMtime(file->inode);
}

void OpenFiles::Evit(size_t size) {
    Ino ino;
    std::shared_ptr<OpenFile> file;
    while (files_->Size() + size >= option_.lruSize) {
        bool yes = files_->GetLast(&ino, &file);
        if (!yes) {
            break;
        }
        Delete(ino, file, true);
    }
}

void OpenFiles::Open(Ino ino, std::shared_ptr<InodeWrapper> inode) {
    WriteLockGuard lk(rwlock_);

    Evit(1);

    std::shared_ptr<OpenFile> file;
    bool yes = files_->Get(ino, &file);
    if (yes) {
        file->refs++;
        return;
    }

    file = std::make_shared<OpenFile>(inode);
    file->refs++;
    files_->Put(ino, file);

    VLOG(1) << "Insert open file cache: ino = " << ino
            << ", refs = " << file->refs
            << ", mtime = " << InodeMtime(file->inode);
}

bool OpenFiles::IsOpened(Ino ino, std::shared_ptr<InodeWrapper>* inode) {
    ReadLockGuard lk(rwlock_);
    std::shared_ptr<OpenFile> file;
    bool yes = files_->Get(ino, &file);
    if (!yes) {
        return false;
    }
    *inode = file->inode;
    return true;
}

void OpenFiles::Close(Ino ino) {
    WriteLockGuard lk(rwlock_);
    std::shared_ptr<OpenFile> file;
    bool yes = files_->Get(ino, &file);
    if (!yes) {
        return;
    }

    if (file->refs > 0) {
        file->refs--;
    }

    if (file->refs == 0) {
        Delete(ino, file, false);  // file already flushed before close
    }
}

/*
 * CloseAll() does:
 *   flush all file to server and delete from LRU cache
 */
void OpenFiles::CloseAll() {
    WriteLockGuard lk(rwlock_);
    Evit(option_.lruSize);
}

bool OpenFiles::GetFileAttr(Ino ino, InodeAttr* attr) {
    ReadLockGuard lk(rwlock_);
    std::shared_ptr<OpenFile> file;
    bool yes = files_->Get(ino, &file);
    if (!yes) {
        return false;
    }

    file->inode->GetInodeAttr(attr);
    return true;
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
