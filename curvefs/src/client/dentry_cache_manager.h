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

#ifndef CURVEFS_SRC_CLIENT_DENTRY_CACHE_MANAGER_H_
#define CURVEFS_SRC_CLIENT_DENTRY_CACHE_MANAGER_H_

#include <memory>
#include <string>
#include <list>
#include <unordered_map>

#include "curvefs/src/client/metaserver_client.h"

#include "src/common/concurrent/concurrent.h"

using ::curvefs::metaserver::Dentry;

namespace curvefs {
namespace client {

class DentryCacheManager {
 public:
    DentryCacheManager() : fsId_(0) {}
    virtual ~DentryCacheManager() {}

    CURVEFS_ERROR Init(uint32_t fsId) {
        fsId_ = fsId;
    }

    virtual CURVEFS_ERROR GetDentry(uint64_t parent,
        const std::string &name, Dentry *out) = 0;

    virtual CURVEFS_ERROR CreateDentry(const Dentry &dentry) = 0;

    virtual CURVEFS_ERROR DeleteDentry(uint64_t parent,
        const std::string &name) = 0;

    virtual CURVEFS_ERROR ListDentry(uint64_t parent,
        std::list<Dentry> *dentryList) = 0;

 protected:
    uint32_t fsId_;
};

const uint32_t kMaxListDentryCount = 1024;

class DentryCacheManagerImpl : public DentryCacheManager {
 public:
    DentryCacheManagerImpl()
      : metaClient_(std::make_shared<MetaServerClientImpl>()),
        maxListCount_(kMaxListDentryCount) {}

    explicit DentryCacheManagerImpl(
        const std::shared_ptr<MetaServerClient> &metaClient)
      : metaClient_(metaClient),
        maxListCount_(kMaxListDentryCount) {}

    CURVEFS_ERROR GetDentry(uint64_t parent,
        const std::string &name, Dentry *out) override;

    CURVEFS_ERROR CreateDentry(const Dentry &dentry) override;

    CURVEFS_ERROR DeleteDentry(uint64_t parent,
        const std::string &name) override;

    CURVEFS_ERROR ListDentry(uint64_t parent,
        std::list<Dentry> *dentryList) override;

 private:
    std::shared_ptr<MetaServerClient> metaClient_;

    std::unordered_map<uint64_t,
        std::unordered_map<std::string, Dentry> > dCache_;

    uint32_t maxListCount_;

    curve::common::Mutex mtx_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_DENTRY_CACHE_MANAGER_H_
