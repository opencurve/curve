/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Project: curve
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include "curvefs/src/client/dir_buffer.h"

namespace curvefs {
namespace client {


uint32_t DirBuffer::DirBufferNew() {
    uint32_t dindex = -1;
    {
        curve::common::LockGuard lg(indexMtx_);
        if (recycleIndex_.empty()) {
            dindex = index_++;
        } else {
            dindex = recycleIndex_.front();
            recycleIndex_.pop_front();
        }
    }
    curve::common::WriteLockGuard wlg(bufferMtx_);
    DirBufferHead *head = new DirBufferHead();
    buffer_.emplace(dindex, head);
    return dindex;
}

DirBufferHead* DirBuffer::DirBufferGet(uint32_t dindex) {
    curve::common::ReadLockGuard rlg(bufferMtx_);
    auto it = buffer_.find(dindex);
    if (it != buffer_.end()) {
        return it->second;
    } else {
        return nullptr;
    }
}

void DirBuffer::DirBufferRelease(uint32_t dindex) {
    {
        curve::common::WriteLockGuard wlg(bufferMtx_);
        auto it = buffer_.find(dindex);
        if (it != buffer_.end()) {
            free(it->second->p);
            delete it->second;
            buffer_.erase(it);
        }
    }
    curve::common::LockGuard lg(indexMtx_);
    recycleIndex_.push_back(dindex);
    return;
}

void DirBuffer::DirBufferFreeAll() {
    curve::common::WriteLockGuard wlg(bufferMtx_);
    curve::common::LockGuard lg(indexMtx_);
    for (auto it : buffer_) {
        free(it.second->p);
        delete it.second;
    }
    buffer_.clear();
    recycleIndex_.clear();
    index_ = 0;
    return;
}






}  // namespace client
}  // namespace curvefs
