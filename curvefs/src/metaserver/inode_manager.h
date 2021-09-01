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
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/trash.h"

namespace curvefs {
namespace metaserver {
class InodeManager {
 public:
    InodeManager(const std::shared_ptr<InodeStorage> &inodeStorage,
        const std::shared_ptr<Trash> &trash)
        : inodeStorage_(inodeStorage),
          trash_(trash),
          nextInodeId_(2) {}

    MetaStatusCode CreateInode(uint32_t fsId, uint64_t length, uint32_t uid,
                               uint32_t gid, uint32_t mode, FsFileType type,
                               const std::string &symlink, Inode *inode);
    MetaStatusCode CreateInode(uint32_t fsId, uint64_t inodeId, uint64_t length,
                               uint32_t uid, uint32_t gid, uint32_t mode,
                               FsFileType type, const std::string &symlink,
                               Inode *inode);
    MetaStatusCode CreateRootInode(uint32_t fsId, uint32_t uid, uint32_t gid,
                                   uint32_t mode);
    MetaStatusCode GetInode(uint32_t fsId, uint64_t inodeId, Inode *inode);

    MetaStatusCode DeleteInode(uint32_t fsId, uint64_t inodeId);

    MetaStatusCode UpdateInode(const Inode &inode);

    MetaStatusCode UpdateInodeVersion(uint32_t fsId, uint64_t inodeId,
                                       uint64_t *version);

    MetaStatusCode InsertInode(const Inode &inode);

 private:
    void GenerateInodeInternal(uint64_t inodeId, uint32_t fsId, uint64_t length,
                               uint32_t uid, uint32_t gid, uint32_t mode,
                               FsFileType type, Inode *inode);

 private:
    uint64_t GetNextId();
    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<Trash> trash_;
    std::atomic<uint64_t> nextInodeId_;
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_INODE_MANAGER_H_
