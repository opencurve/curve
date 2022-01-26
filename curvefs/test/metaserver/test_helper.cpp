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
 * Date: 2021-12-28
 * Author: xuchaojie
 */

#include "curvefs/test/metaserver/test_helper.h"

namespace curvefs {
namespace metaserver {

UpdateInodeRequest MakeUpdateInodeRequestFromInode(const Inode &inode,
    uint32_t poolId, uint32_t copysetId, uint32_t partitionId) {
    UpdateInodeRequest request;
    request.set_poolid(poolId);
    request.set_copysetid(copysetId);
    request.set_partitionid(partitionId);
    request.set_fsid(inode.fsid());
    request.set_inodeid(inode.inodeid());
    request.set_length(inode.length());
    request.set_ctime(inode.ctime());
    request.set_ctime_ns(inode.ctime_ns());
    request.set_mtime(inode.mtime());
    request.set_mtime_ns(inode.mtime_ns());
    request.set_atime(inode.atime());
    request.set_atime_ns(inode.atime_ns());
    request.set_uid(inode.uid());
    request.set_gid(inode.gid());
    request.set_mode(inode.mode());
    VolumeExtentList *vlist =
        new VolumeExtentList;
    vlist->CopyFrom(inode.volumeextentlist());
    request.set_allocated_volumeextentlist(vlist);
    *(request.mutable_s3chunkinfomap()) = inode.s3chunkinfomap();
    *(request.mutable_xattr()) = inode.xattr();
    request.set_nlink(inode.nlink());
    return request;
}

}  // namespace metaserver
}  // namespace curvefs
