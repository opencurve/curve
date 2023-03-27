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
 * Date: Friday Apr 22 17:02:33 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_CLIENT_ASYNC_REQUEST_CLOSURE_H_
#define CURVEFS_SRC_CLIENT_ASYNC_REQUEST_CLOSURE_H_

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <memory>

#include "curvefs/src/client/rpcclient/task_excutor.h"
#include "curvefs/src/client/filesystem/error.h"

namespace curvefs {
namespace client {

using ::curvefs::client::filesystem::CURVEFS_ERROR;
using ::curvefs::client::filesystem::ToFSError;

class InodeWrapper;

namespace internal {

class AsyncRequestClosureBase : public rpcclient::MetaServerClientDone {
 public:
    explicit AsyncRequestClosureBase(
        const std::shared_ptr<InodeWrapper>& inode);

    ~AsyncRequestClosureBase() override;

 protected:
    std::shared_ptr<InodeWrapper> inode_;
};

}  // namespace internal

// Callback of UpdateVolumeExtent
//
// for synchronous update
//
//     UpdateVolumeExtentClosure closure(
//         shared_ptr of a inode wrapper, /* sync */ true);
//     // xxx
//     auto statusCode = closure.Wait();
//
// for synchronous update
//
//     auto* closure = new UpdateVolumeExtentClosure(
//          shared_ptr of a inode wrapper, /* sync */ false);
//     // no need to call closure.Wait()
//
class UpdateVolumeExtentClosure : public internal::AsyncRequestClosureBase {
 public:
    UpdateVolumeExtentClosure(const std::shared_ptr<InodeWrapper>& inode,
                              bool sync);

    void Run() override;

    // Wait request finished, and return status code
    // Invoked only on sync mode, otherwise the behaviour is undefined
    CURVEFS_ERROR Wait();

 private:
    bool sync_;
    bool done_ = false;
    bthread::Mutex mtx_;
    bthread::ConditionVariable cond_;
};

class UpdateInodeAttrAndExtentClosure
    : public internal::AsyncRequestClosureBase {
 public:
    using Base = AsyncRequestClosureBase;

    UpdateInodeAttrAndExtentClosure(const std::shared_ptr<InodeWrapper>& inode,
                                    MetaServerClientDone* parent);

    void Run() override;

 private:
    MetaServerClientDone* parent_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_ASYNC_REQUEST_CLOSURE_H_
