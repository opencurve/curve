/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Friday Apr 22 17:04:35 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/client/async_request_closure.h"

#include <bthread/mutex.h>

#include <memory>
#include <mutex>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/filesystem/error.h"
#include "curvefs/src/client/inode_wrapper.h"

namespace curvefs {
namespace client {

namespace internal {

AsyncRequestClosureBase::AsyncRequestClosureBase(
    const std::shared_ptr<InodeWrapper>& inode)
    : inode_(inode) {}

AsyncRequestClosureBase::~AsyncRequestClosureBase() = default;

}  // namespace internal

namespace {
bool IsOK(MetaStatusCode code) {
    return code == MetaStatusCode::OK || code == MetaStatusCode::NOT_FOUND;
}
}  // namespace

UpdateVolumeExtentClosure::UpdateVolumeExtentClosure(
    const std::shared_ptr<InodeWrapper>& inode,
    bool sync)
    : AsyncRequestClosureBase(inode), sync_(sync) {}

CURVEFS_ERROR UpdateVolumeExtentClosure::Wait() {
    assert(sync_);

    std::unique_lock<bthread::Mutex> lk(mtx_);
    while (!done_) {
        cond_.wait(lk);
    }

    return ToFSError(GetStatusCode());
}

void UpdateVolumeExtentClosure::Run() {
    auto st = GetStatusCode();
    if (!IsOK(st)) {
        LOG(ERROR) << "UpdateVolumeExtent failed, error: "
                   << MetaStatusCode_Name(st)
                   << ", inodeid: " << inode_->GetInodeId();
        inode_->MarkInodeError();
    }

    inode_->syncingVolumeExtentsMtx_.unlock();

    if (sync_) {
        std::lock_guard<bthread::Mutex> lk(mtx_);
        done_ = true;
        cond_.notify_one();
    } else {
        delete this;
    }
}

UpdateInodeAttrAndExtentClosure::UpdateInodeAttrAndExtentClosure(
    const std::shared_ptr<InodeWrapper>& inode,
    MetaServerClientDone* parent)
    : Base(inode), parent_(parent) {}

void UpdateInodeAttrAndExtentClosure::Run() {
    std::unique_ptr<UpdateInodeAttrAndExtentClosure> guard(this);

    auto st = GetStatusCode();
    if (!IsOK(st)) {
        LOG(ERROR) << "UpdateInodeAttrAndExtent failed, error: "
                   << MetaStatusCode_Name(st)
                   << ", inode: " << inode_->GetInodeId();
        inode_->MarkInodeError();
    }
    inode_->ClearDirty();
    inode_->syncingVolumeExtentsMtx_.unlock();
    inode_->ReleaseSyncingInode();

    if (parent_ != nullptr) {
        parent_->SetMetaStatusCode(st);
        parent_->Run();
    }
}

}  // namespace client
}  // namespace curvefs
