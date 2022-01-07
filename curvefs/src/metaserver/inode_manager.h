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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef CURVEFS_SRC_METASERVER_INODE_MANAGER_H_
#define CURVEFS_SRC_METASERVER_INODE_MANAGER_H_

#include <atomic>
#include <memory>
#include <string>
#include <list>
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/trash.h"
#include "src/common/concurrent/name_lock.h"

using ::curve::common::NameLock;

namespace curvefs {
namespace metaserver {
class InodeManager {
 public:
    InodeManager(const std::shared_ptr<InodeStorage> &inodeStorage,
        const std::shared_ptr<Trash> &trash)
        : inodeStorage_(inodeStorage),
          trash_(trash) {}

    MetaStatusCode CreateInode(uint32_t fsId, uint64_t inodeId, uint64_t length,
                               uint32_t uid, uint32_t gid, uint32_t mode,
                               FsFileType type, const std::string &symlink,
                               uint64_t rdev, Inode *inode);
    MetaStatusCode CreateRootInode(uint32_t fsId, uint32_t uid, uint32_t gid,
                                   uint32_t mode);
    MetaStatusCode GetInode(uint32_t fsId, uint64_t inodeId, Inode *inode);

    MetaStatusCode DeleteInode(uint32_t fsId, uint64_t inodeId);

    MetaStatusCode UpdateInode(const UpdateInodeRequest &request);

    MetaStatusCode GetOrModifyS3ChunkInfo(uint32_t fsId, uint64_t inodeId,
        const google::protobuf::Map<uint64_t, S3ChunkInfoList> &s3ChunkInfoAdd,
        const google::protobuf::Map<uint64_t, S3ChunkInfoList>
            &s3ChunkInfoRemove,
        bool returnInode,
        Inode *out);

    MetaStatusCode UpdateInodeWhenCreateOrRemoveSubNode(uint32_t fsId,
        uint64_t inodeId, bool isCreate);

    MetaStatusCode InsertInode(const Inode &inode);

    void GetInodeIdList(std::list<uint64_t>* inodeIdList);

 private:
    void GenerateInodeInternal(uint64_t inodeId, uint32_t fsId, uint64_t length,
                               uint32_t uid, uint32_t gid, uint32_t mode,
                               FsFileType type, uint64_t rdev, Inode *inode);

    std::string GetInodeLockName(uint32_t fsId, uint64_t inodeId) {
        return std::to_string(fsId) + "_" + std::to_string(inodeId);
    }

 private:
    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<Trash> trash_;

    NameLock inodeLock_;
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_INODE_MANAGER_H_
