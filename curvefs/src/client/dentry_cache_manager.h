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

#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/src/client/error_code.h"
#include "src/common/concurrent/concurrent.h"

using ::curvefs::metaserver::Dentry;

namespace curvefs {
namespace client {

using rpcclient::MetaServerClient;
using rpcclient::MetaServerClientImpl;
using common::DCacheOption;

class DentryCacheManager {
 public:
    DentryCacheManager() : fsId_(0) {}
    virtual ~DentryCacheManager() {}

    virtual CURVEFS_ERROR Init(const DCacheOption &option) = 0;

    void SetFsId(uint32_t fsId) {
        fsId_ = fsId;
    }

    virtual void InsertOrReplaceCache(const Dentry& dentry,
                                      bool replace) = 0;

    virtual void DeleteCache(uint64_t parentId, const std::string& name) = 0;

    virtual CURVEFS_ERROR GetTxId(uint32_t fsId,
                                  uint64_t inodeId,
                                  uint32_t* partitionId,
                                  uint64_t* txId) = 0;

    virtual void SetTxId(uint32_t partitionId,
                         uint64_t txId) = 0;

    virtual CURVEFS_ERROR GetDentry(uint64_t parent,
        const std::string &name, Dentry *out) = 0;

    virtual CURVEFS_ERROR CreateDentry(const Dentry &dentry) = 0;

    virtual CURVEFS_ERROR DeleteDentry(uint64_t parent,
        const std::string &name) = 0;

    virtual CURVEFS_ERROR ListDentry(uint64_t parent,
        std::list<Dentry> *dentryList, uint32_t limit) = 0;

    virtual CURVEFS_ERROR Rename(const Dentry& srcDentry,
                                 const Dentry& dstDentry) = 0;

 protected:
    uint32_t fsId_;
};

class DentryCacheManagerImpl : public DentryCacheManager {
 public:
    using Hash = std::unordered_map<std::string, Dentry>;

 public:
    DentryCacheManagerImpl()
      : metaClient_(std::make_shared<MetaServerClientImpl>()) {}

    explicit DentryCacheManagerImpl(
        const std::shared_ptr<MetaServerClient> &metaClient)
      : metaClient_(metaClient) {}

    CURVEFS_ERROR Init(const DCacheOption &option) override;

    void InsertOrReplaceCache(const Dentry& dentry,
                              bool replace) override;

    void DeleteCache(uint64_t parentId, const std::string& name) override;

    CURVEFS_ERROR GetTxId(uint32_t fsId,
                          uint64_t inodeId,
                          uint32_t* partitionId,
                          uint64_t* txId) override;

    void SetTxId(uint32_t partitionId, uint64_t txId) override;

    CURVEFS_ERROR GetDentry(uint64_t parent,
        const std::string &name, Dentry *out) override;

    CURVEFS_ERROR CreateDentry(const Dentry &dentry) override;

    CURVEFS_ERROR DeleteDentry(uint64_t parent,
        const std::string &name) override;

    CURVEFS_ERROR ListDentry(uint64_t parent,
        std::list<Dentry> *dentryList, uint32_t limit) override;

    CURVEFS_ERROR Rename(const Dentry& srcDentry,
                         const Dentry& dstDentry) override;

 private:
    std::shared_ptr<MetaServerClient> metaClient_;

    std::unordered_map<uint64_t, Hash> dCache_;

    curve::common::RWLock mtx_;

    uint32_t maxListDentryCount_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_DENTRY_CACHE_MANAGER_H_
