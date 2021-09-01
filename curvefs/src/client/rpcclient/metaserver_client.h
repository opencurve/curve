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
 * Created Date: Mon Sept 1 2021
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_CLIENT_RPCCLIENT_METASERVER_CLIENT_H_
#define CURVEFS_SRC_CLIENT_RPCCLIENT_METASERVER_CLIENT_H_

#include <list>
#include <memory>
#include <string>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/client/rpcclient/base_client.h"
#include "curvefs/src/client/common/config.h"

using ::curvefs::metaserver::Dentry;
using ::curvefs::metaserver::FsFileType;
using ::curvefs::metaserver::Inode;
using ::curvefs::space::AllocateType;
using ::curvefs::metaserver::MetaStatusCode;
using ::curvefs::client::common::MetaServerOption;

namespace curvefs {
namespace client {
namespace rpcclient {


class MetaServerClient {
 public:
    MetaServerClient() {}

    virtual ~MetaServerClient() {}

    virtual MetaStatusCode Init(const MetaServerOption &metaopt,
                               MetaServerBaseClient *baseclient) = 0;

    virtual MetaStatusCode GetDentry(uint32_t fsId, uint64_t inodeid,
                                    const std::string &name, Dentry *out) = 0;

    virtual MetaStatusCode ListDentry(uint32_t fsId, uint64_t inodeid,
                                     const std::string &last, uint32_t count,
                                     std::list<Dentry> *dentryList) = 0;

    virtual MetaStatusCode CreateDentry(const Dentry &dentry) = 0;

    virtual MetaStatusCode DeleteDentry(uint32_t fsId, uint64_t inodeid,
                                       const std::string &name) = 0;

    virtual MetaStatusCode GetInode(uint32_t fsId, uint64_t inodeid,
                                   Inode *out) = 0;

    virtual MetaStatusCode UpdateInode(const Inode &inode) = 0;

    virtual MetaStatusCode CreateInode(const InodeParam &param, Inode *out) = 0;

    virtual MetaStatusCode DeleteInode(uint32_t fsId, uint64_t inodeid) = 0;
};


class MetaServerClientImpl : public MetaServerClient {
 public:
    MetaServerClientImpl() {}

    MetaStatusCode Init(const MetaServerOption &metaopt,
                       MetaServerBaseClient *baseclient) override;

    MetaStatusCode GetDentry(uint32_t fsId, uint64_t inodeid,
                            const std::string &name, Dentry *out) override;

    MetaStatusCode ListDentry(uint32_t fsId, uint64_t inodeid,
                             const std::string &last, uint32_t count,
                             std::list<Dentry> *dentryList) override;

    MetaStatusCode CreateDentry(const Dentry &dentry) override;

    MetaStatusCode DeleteDentry(uint32_t fsId, uint64_t inodeid,
                               const std::string &name) override;

    MetaStatusCode GetInode(uint32_t fsId, uint64_t inodeid,
                           Inode *out) override;

    MetaStatusCode UpdateInode(const Inode &inode) override;

    MetaStatusCode CreateInode(const InodeParam &param, Inode *out) override;

    MetaStatusCode DeleteInode(uint32_t fsId, uint64_t inodeid) override;
};
}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_METASERVER_CLIENT_H_
