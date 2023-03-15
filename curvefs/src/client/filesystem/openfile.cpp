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

#include "curvefs/src/client/filesystem/core.h"

namespace curvefs {
namespace client {
namespace filesystem {

OpenFiles::OpenFiles(OpenFileOption option)
    : rwlock_(),
      lruSize_(option.lruSize),
      files_(std::make_shared<LRUType>(option.lruSize)) {
    mq_ = std::make_shared<MessageQueueType>("openfile", 10000);
    mq_->Sub([&](const std::shared_ptr<OpenFile>& file){
        Sync(file);
    });
    mq_->Run();
}

void OpenFiles::Delete(Ino ino, const std::shared_ptr<OpenFile>& file) {
    mq_->Pub(file);
    files_->Remove(ino);
    // TODO: update directory cache
}

void OpenFiles::Evit() {
    Ino ino;
    std::shared_ptr<OpenFile> file;
    while (files_->Size() >= lruSize_) {
        bool yes = files_->GetLast(&ino, &file);
        if (!yes) {
            break;
        }
        Delete(ino, file);
    }
}

void OpenFiles::Open(Ino ino, std::shared_ptr<InodeWrapper> inode) {
    WriteLockGuard lk(rwlock_);

    Evit();

    std::shared_ptr<OpenFile> file;
    bool yes = files_->Get(ino, &file);
    if (!yes) {
        file = std::make_shared<OpenFile>(inode);
        files_->Put(ino, file);
    }
    file->refs++;
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
        Delete(ino, file);
    }
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

bool OpenFiles::GetLength(Ino ino, uint64_t* length) {
    ReadLockGuard lk(rwlock_);
    std::shared_ptr<OpenFile> file;
    bool yes = files_->Get(ino, &file);
    if (!yes) {
        return false;
    }
    *length = file->inode->GetLength();
    return true;
}

void OpenFiles::Sync(const std::shared_ptr<OpenFile>& file) {
    auto inode = file->inode;
    UniqueLock lk(inode->GetUniqueLock());
    inode->Sync(true);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
