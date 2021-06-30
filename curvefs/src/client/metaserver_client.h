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

#ifndef CURVEFS_SRC_CLIENT_METASERVER_CLIENT_H_
#define CURVEFS_SRC_CLIENT_METASERVER_CLIENT_H_

#include <list>
#include <memory>
#include <string>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/client/base_client.h"
#include "curvefs/src/client/config.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/extent.h"

using ::curvefs::metaserver::Dentry;
using ::curvefs::metaserver::FsFileType;
using ::curvefs::metaserver::Inode;
using ::curvefs::space::AllocateType;

namespace curvefs {
namespace client {

using curvefs::metaserver::MetaStatusCode;

class MetaServerClient {
 public:
    MetaServerClient() {}

    virtual ~MetaServerClient() {}

    virtual CURVEFS_ERROR Init(const MetaServerOption &metaopt,
                       MetaServerBaseClient *baseclient) = 0;

    virtual CURVEFS_ERROR GetDentry(uint32_t fsId, uint64_t inodeid,
                                    const std::string &name, Dentry *out) = 0;

    virtual CURVEFS_ERROR ListDentry(uint32_t fsId, uint64_t inodeid,
                                     const std::string &last, uint32_t count,
                                     std::list<Dentry> *dentryList) = 0;

    virtual CURVEFS_ERROR CreateDentry(const Dentry &dentry) = 0;

    virtual CURVEFS_ERROR DeleteDentry(uint32_t fsId, uint64_t inodeid,
                                       const std::string &name) = 0;

    virtual CURVEFS_ERROR GetInode(uint32_t fsId, uint64_t inodeid,
                                   Inode *out) = 0;

    virtual CURVEFS_ERROR UpdateInode(const Inode &inode) = 0;

    virtual CURVEFS_ERROR CreateInode(const InodeParam &param, Inode *out) = 0;

    virtual CURVEFS_ERROR DeleteInode(uint32_t fsId, uint64_t inodeid) = 0;
};


class MetaServerClientImpl : public MetaServerClient {
 public:
    MetaServerClientImpl() {}

    using RPCFunc =
        std::function<CURVEFS_ERROR(brpc::Channel *, brpc::Controller *)>;

    CURVEFS_ERROR Init(const MetaServerOption &metaopt,
                       MetaServerBaseClient *baseclient) override;

    CURVEFS_ERROR GetDentry(uint32_t fsId, uint64_t inodeid,
                            const std::string &name, Dentry *out) override;

    CURVEFS_ERROR ListDentry(uint32_t fsId, uint64_t inodeid,
                             const std::string &last, uint32_t count,
                             std::list<Dentry> *dentryList) override;

    CURVEFS_ERROR CreateDentry(const Dentry &dentry) override;

    CURVEFS_ERROR DeleteDentry(uint32_t fsId, uint64_t inodeid,
                               const std::string &name) override;

    CURVEFS_ERROR GetInode(uint32_t fsId, uint64_t inodeid,
                           Inode *out) override;

    CURVEFS_ERROR UpdateInode(const Inode &inode) override;

    CURVEFS_ERROR CreateInode(const InodeParam &param, Inode *out) override;

    CURVEFS_ERROR DeleteInode(uint32_t fsId, uint64_t inodeid) override;

    void MetaServerStatusCode2CurveFSErr(const MetaStatusCode &statcode,
                                         CURVEFS_ERROR *errcode);

 protected:
    class MetaServerRPCExcutor {
     public:
        MetaServerRPCExcutor() : opt_() {}
        ~MetaServerRPCExcutor() {}
        void SetOption(const MetaServerOption &option) { opt_ = option; }

        CURVEFS_ERROR DoRPCTask(RPCFunc task);
     private:
        MetaServerOption opt_;
    };

 private:
    MetaServerBaseClient *basecli_;
    MetaServerRPCExcutor excutor_;
};


}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_METASERVER_CLIENT_H_
