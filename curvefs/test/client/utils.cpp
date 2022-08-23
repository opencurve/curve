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

#include "curvefs/test/client/utils.h"

#include "curvefs/proto/metaserver.pb.h"

namespace curvefs {
namespace client {

InodeAttr ToInodeAttr(const Inode& inode) {
    InodeAttr attr;

    attr.set_inodeid(inode.inodeid());
    attr.set_fsid(inode.fsid());
    attr.set_length(inode.length());
    attr.set_ctime(inode.ctime());
    attr.set_ctime_ns(inode.ctime_ns());
    attr.set_mtime(inode.mtime());
    attr.set_mtime_ns(inode.mtime_ns());
    attr.set_atime(inode.atime());
    attr.set_atime_ns(inode.atime_ns());

    attr.set_uid(inode.uid());
    attr.set_gid(inode.gid());
    attr.set_mode(inode.mode());
    attr.set_nlink(inode.nlink());
    attr.set_type(inode.type());

    if (inode.has_symlink()) {
        attr.set_symlink(inode.symlink());
    }

    if (inode.has_rdev()) {
        attr.set_rdev(inode.rdev());
    }

    if (inode.has_dtime()) {
        attr.set_dtime(inode.dtime());
    }

    if (inode.has_openmpcount()) {
        attr.set_openmpcount(inode.openmpcount());
    }

    if (!inode.xattr().empty()) {
        *attr.mutable_xattr() = inode.xattr();
    }

    if (!inode.parent().empty()) {
        *attr.mutable_parent() = inode.parent();
    }

    return attr;
}

}  // namespace client
}  // namespace curvefs
