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

#ifndef CURVEFS_SRC_CLIENT_INODE_CACHE_MANAGER_H_
#define CURVEFS_SRC_CLIENT_INODE_CACHE_MANAGER_H_

#include <memory>
#include <unordered_map>

#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/src/client/error_code.h"
#include "src/common/concurrent/concurrent.h"
#include "curvefs/src/client/inode_wrapper.h"

namespace curvefs {
namespace client {

using rpcclient::MetaServerClient;
using rpcclient::MetaServerClientImpl;
using rpcclient::InodeParam;

class InodeCacheManager {
 public:
    InodeCacheManager()
      : fsId_(0) {}
    virtual ~InodeCacheManager() {}

    void SetFsId(uint32_t fsId) {
        fsId_ = fsId;
    }

    virtual CURVEFS_ERROR GetInode(uint64_t inodeid,
        std::shared_ptr<InodeWapper> &out) = 0;   // NOLINT

    virtual CURVEFS_ERROR CreateInode(const InodeParam &param,
        std::shared_ptr<InodeWapper> &out) = 0;   // NOLINT

    virtual CURVEFS_ERROR DeleteInode(uint64_t inodeid) = 0;

 protected:
    uint32_t fsId_;
};

class InodeCacheManagerImpl : public InodeCacheManager {
 public:
    InodeCacheManagerImpl()
      : metaClient_(std::make_shared<MetaServerClientImpl>()) {}

    explicit InodeCacheManagerImpl(
        const std::shared_ptr<MetaServerClient> &metaClient)
      : metaClient_(metaClient) {}

    CURVEFS_ERROR GetInode(uint64_t inodeid,
        std::shared_ptr<InodeWapper> &out) override;    // NOLINT

    CURVEFS_ERROR CreateInode(const InodeParam &param,
        std::shared_ptr<InodeWapper> &out) override;    // NOLINT

    CURVEFS_ERROR DeleteInode(uint64_t inodeid) override;

 private:
    std::shared_ptr<MetaServerClient> metaClient_;
    std::unordered_map<uint64_t, std::shared_ptr<InodeWapper>> iCache_;
    curve::common::RWLock mtx_;
};


}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_INODE_CACHE_MANAGER_H_
