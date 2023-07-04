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
 * Created Date: 2023-03-07
 * Author: Jingli Chen (Wine93)
 */

#include <map>
#include <set>
#include <list>
#include <string>
#include <memory>
#include <utility>

#include "curvefs/src/client/filesystem/rpc_client.h"

namespace curvefs {
namespace client {
namespace filesystem {

RPCClient::RPCClient(RPCOption option,
                     ExternalMember member)
    : option_(option),
      inodeManager_(member.inodeManager),
      dentryManager_(member.dentryManager) {}

CURVEFS_ERROR RPCClient::GetAttr(Ino ino, InodeAttr* attr) {
    CURVEFS_ERROR rc = inodeManager_->GetInodeAttr(ino, attr);
    if (rc != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "rpc(getattr::GetInodeAttr) failed, retCode = " << rc
                   << ", ino = " << ino;
    }
    return rc;
}

CURVEFS_ERROR RPCClient::Lookup(Ino parent,
                                const std::string& name,
                                EntryOut* entryOut) {
    Dentry dentry;
    CURVEFS_ERROR rc = dentryManager_->GetDentry(parent, name, &dentry);
    if (rc != CURVEFS_ERROR::OK) {
        if (rc != CURVEFS_ERROR::NOT_EXIST) {
            LOG(ERROR) << "rpc(lookup::GetDentry) failed, retCode = " << rc
                       << ", parent = " << parent << ", name = " << name;
        }
        return rc;
    }

    Ino ino = dentry.inodeid();
    rc = inodeManager_->GetInodeAttr(ino, &entryOut->attr);
    if (rc != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "rpc(lookup::GetInodeAttr) failed, retCode = " << rc
                   << ", ino = " << ino;
    }
    return rc;
}

CURVEFS_ERROR RPCClient::ReadDir(Ino ino,
                                 std::shared_ptr<DirEntryList>* entries) {
    uint32_t limit = option_.listDentryLimit;

    std::list<Dentry> dentries;
    CURVEFS_ERROR rc = dentryManager_->ListDentry(ino, &dentries, limit);
    if (rc != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "rpc(readdir::ListDentry) failed, retCode = " << rc
                   << ", ino = " << ino;
        return rc;
    }

    std::set<uint64_t> inos;
    std::map<uint64_t, InodeAttr> attrs;
    std::for_each(dentries.begin(), dentries.end(), [&](Dentry& dentry){
        inos.emplace(dentry.inodeid());
    });
    rc = inodeManager_->BatchGetInodeAttrAsync(ino, &inos, &attrs);
    if (rc != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "rpc(readdir::BatchGetInodeAttrAsync) failed"
                   << ", retCode = " << rc << ", ino = " << ino;
        return rc;
    }

    DirEntry dirEntry;
    for (const auto& dentry : dentries) {
        Ino ino = dentry.inodeid();
        auto iter = attrs.find(ino);
        if (iter == attrs.end()) {
            LOG(WARNING) << "rpc(readdir::BatchGetInodeAttrAsync) "
                         << "missing attribute, ino = " << ino;
            continue;
        }

        // NOTE: we can't use std::move() for attribute for hard link
        // which will sharing inode attribute.
        dirEntry.ino = ino;
        dirEntry.name = std::move(dentry.name());
        dirEntry.attr = iter->second;
        (*entries)->Add(dirEntry);
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR RPCClient::Open(Ino ino, std::shared_ptr<InodeWrapper>* inode) {
    CURVEFS_ERROR rc = inodeManager_->GetInode(ino, *inode);
    if (rc != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "rpc(open/GetInode) failed" << ", retCode = " << rc
                   << ", ino = " << ino;
    }
    return rc;
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
