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
 * Created Date: Thur Sept 3 2021
 * Author: lixiaocui
 */


#ifndef CURVEFS_SRC_CLIENT_COMMON_COMMON_H_
#define CURVEFS_SRC_CLIENT_COMMON_COMMON_H_

#include <bthread/mutex.h>
#include <ostream>

namespace curvefs {
namespace client {
namespace common {

using MetaserverID = uint32_t;
using PartitionID = uint32_t;

using Mutex = ::bthread::Mutex;

enum DiskCacheType {
    Disable = 0,
    OnlyRead = 1,
    ReadWrite = 2
};

enum class MetaServerOpType {
    GetDentry,
    ListDentry,
    CreateDentry,
    DeleteDentry,
    PrepareRenameTx,
    GetInode,
    BatchGetInodeAttr,
    BatchGetXAttr,
    UpdateInode,
    CreateInode,
    DeleteInode,
    GetOrModifyS3ChunkInfo,
    GetVolumeExtent,
    UpdateVolumeExtent,
};

std::ostream &operator<<(std::ostream &os, MetaServerOpType optype);

const uint32_t MAXXATTRLENGTH = 256;

enum class FileHandle : uint64_t {
    kDefaultValue = 0,
    kKeepCache = 1,
};

}  // namespace common
}  // namespace client
}  // namespace curvefs


#endif  // CURVEFS_SRC_CLIENT_COMMON_COMMON_H_
