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
#include <vector>
#include <list>
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/trash.h"
#include "src/common/concurrent/name_lock.h"

using ::curve::common::NameLock;
using ::curvefs::metaserver::S3ChunkInfoList;

namespace curvefs {
namespace metaserver {

using FileType2InodeNumMap =
    ::google::protobuf::Map<::google::protobuf::int32,
                            ::google::protobuf::uint64>;

struct InodeParam {
    uint32_t fsId;
    uint64_t length;
    uint32_t uid;
    uint32_t gid;
    uint32_t mode;
    FsFileType type;
    std::string symlink;
    uint64_t rdev;
    uint64_t parent;
};

class InodeManager {
 public:
    InodeManager(const std::shared_ptr<InodeStorage>& inodeStorage,
                 const std::shared_ptr<Trash>& trash,
                 FileType2InodeNumMap* type2InodeNum)
        : inodeStorage_(inodeStorage),
          trash_(trash),
          type2InodeNum_(type2InodeNum) {}

    MetaStatusCode CreateInode(uint64_t inodeId, const InodeParam &param,
                               Inode *inode);
    MetaStatusCode CreateRootInode(const InodeParam &param);

    MetaStatusCode GetInode(uint32_t fsId,
                            uint64_t inodeId,
                            Inode *inode,
                            bool paddingS3ChunkInfo = false);

    MetaStatusCode GetInodeAttr(uint32_t fsId, uint64_t inodeId,
                                InodeAttr *attr);

    MetaStatusCode GetXAttr(uint32_t fsId, uint64_t inodeId, XAttr *xattr);

    MetaStatusCode DeleteInode(uint32_t fsId, uint64_t inodeId);

    MetaStatusCode UpdateInode(const UpdateInodeRequest& request);

    MetaStatusCode GetOrModifyS3ChunkInfo(
        uint32_t fsId,
        uint64_t inodeId,
        const S3ChunkInfoMap& map2add,
        const S3ChunkInfoMap& map2del,
        bool returnS3ChunkInfoMap,
        std::shared_ptr<Iterator>* iterator4InodeS3Meta);

    MetaStatusCode PaddingInodeS3ChunkInfo(int32_t fsId,
                                           uint64_t inodeId,
                                           S3ChunkInfoMap* m,
                                           uint64_t limit = 0);

    MetaStatusCode UpdateInodeWhenCreateOrRemoveSubNode(uint32_t fsId,
        uint64_t inodeId, FsFileType type, bool isCreate);

    MetaStatusCode InsertInode(const Inode &inode);

    bool GetInodeIdList(std::list<uint64_t>* inodeIdList);

    // Update one or more volume extent slice
    MetaStatusCode UpdateVolumeExtent(uint32_t fsId,
                                      uint64_t inodeId,
                                      const VolumeExtentList &extents);

    // Update only one volume extent slice
    MetaStatusCode UpdateVolumeExtentSlice(uint32_t fsId,
                                           uint64_t inodeId,
                                           const VolumeExtentSlice &slice);

    MetaStatusCode GetVolumeExtent(uint32_t fsId,
                                   uint64_t inodeId,
                                   const std::vector<uint64_t> &slices,
                                   VolumeExtentList *extents);

 private:
    void GenerateInodeInternal(uint64_t inodeId, const InodeParam &param,
                               Inode *inode);

    bool AppendS3ChunkInfo(uint32_t fsId,
                           uint64_t inodeId,
                           S3ChunkInfoMap added);

    static std::string GetInodeLockName(uint32_t fsId, uint64_t inodeId) {
        return std::to_string(fsId) + "_" + std::to_string(inodeId);
    }

    MetaStatusCode UpdateVolumeExtentSliceLocked(
        uint32_t fsId,
        uint64_t inodeId,
        const VolumeExtentSlice &slice);

 private:
    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<Trash> trash_;
    FileType2InodeNumMap* type2InodeNum_;

    NameLock inodeLock_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_INODE_MANAGER_H_
