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

#include <memory>
#include <list>
#include <string>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/client/extent.h"
#include "curvefs/src/client/error_code.h"

using ::curvefs::metaserver::Dentry;
using ::curvefs::metaserver::Inode;
using ::curvefs::metaserver::FsFileType;
using ::curvefs::space::Extent;

namespace curvefs {
namespace client {

struct InodeParam {
    uint64_t fsId;
    uint64_t length;
    uint32_t uid;
    uint32_t gid;
    uint32_t mode;
    FsFileType type;
    std::string symlink;
};

class MetaServerClient {
 public:
    MetaServerClient() {}

    virtual ~MetaServerClient() {}

    virtual CURVEFS_ERROR GetDentry(uint32_t fsId, uint64_t inodeid,
                  const std::string &name, Dentry *out) = 0;

    virtual CURVEFS_ERROR ListDentry(uint32_t fsId, uint64_t inodeid,
            const std::string &last, uint32_t count) = 0;

    virtual CURVEFS_ERROR UpdateDentry(const Dentry &dentry) = 0;

    virtual CURVEFS_ERROR CreateDentry(const Dentry &dentry) = 0;

    virtual CURVEFS_ERROR DeleteDentry(
            uint32_t fsId, uint64_t inodeid, const std::string &name) = 0;

    virtual CURVEFS_ERROR GetInode(
            uint32_t fsId, uint64_t inodeid, Inode *out) = 0;

    virtual CURVEFS_ERROR UpdateInode(const Inode &inode) = 0;

    virtual CURVEFS_ERROR CreateInode(const InodeParam &param, Inode *out) = 0;

    virtual CURVEFS_ERROR DeleteInode(uint32_t fsId, uint64_t inodeid) = 0;

    virtual CURVEFS_ERROR AllocExtents(uint32_t fsId,
        const std::list<ExtentAllocInfo> &toAllocExtents,
        std::list<Extent> *allocatedExtents) = 0;

    virtual CURVEFS_ERROR DeAllocExtents(uint32_t fsId,
        std::list<Extent> allocatedExtents) = 0;

 private:
    std::string ip_;
    uint32_t port_;
};

class MetaServerClientImpl : public MetaServerClient {
 public:
    MetaServerClientImpl() {}

    CURVEFS_ERROR GetDentry(uint32_t fsId, uint64_t inodeid,
                  const std::string &name, Dentry *out) override;

    CURVEFS_ERROR ListDentry(uint32_t fsId, uint64_t inodeid,
            const std::string &last, uint32_t count) override;

    CURVEFS_ERROR UpdateDentry(const Dentry &dentry) override;

    CURVEFS_ERROR CreateDentry(const Dentry &dentry) override;

    CURVEFS_ERROR DeleteDentry(
            uint32_t fsId, uint64_t inodeid, const std::string &name) override;

    CURVEFS_ERROR GetInode(
            uint32_t fsId, uint64_t inodeid, Inode *out) override;

    CURVEFS_ERROR UpdateInode(const Inode &inode) override;

    CURVEFS_ERROR CreateInode(const InodeParam &param, Inode *out) override;

    CURVEFS_ERROR DeleteInode(uint32_t fsId, uint64_t inodeid) override;

    CURVEFS_ERROR AllocExtents(uint32_t fsId,
        const std::list<ExtentAllocInfo> &toAllocExtents,
        std::list<Extent> *allocatedExtents) override;

    CURVEFS_ERROR DeAllocExtents(uint32_t fsId,
        std::list<Extent> allocatedExtents) override;

 private:
    std::string ip_;
    uint32_t port_;
};


}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_METASERVER_CLIENT_H_
