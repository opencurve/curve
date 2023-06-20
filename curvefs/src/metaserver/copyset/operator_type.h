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
 * Date: Fri Sep  3 17:30:00 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_OPERATOR_TYPE_H_
#define CURVEFS_SRC_METASERVER_COPYSET_OPERATOR_TYPE_H_

#include <cstdint>

namespace curvefs {
namespace metaserver {
namespace copyset {

// OperatorType for raft log
//
// OperatorType is encoded with client's requests into raft log, so remove or
// reorder existing types may introduces compatible issues
enum class OperatorType : uint32_t {
    GetDentry = 0,
    ListDentry = 1,
    CreateDentry = 2,
    DeleteDentry = 3,
    GetInode = 4,
    BatchGetInodeAttr = 5,
    BatchGetXAttr = 6,
    CreateInode = 7,
    UpdateInode = 8,
    DeleteInode = 9,
    CreateRootInode = 10,
    CreatePartition = 11,
    DeletePartition = 12,
    PrepareRenameTx = 13,
    GetOrModifyS3ChunkInfo = 14,
    GetVolumeExtent = 15,
    UpdateVolumeExtent = 16,
    CreateManageInode = 17,
    IsDirEmpty = 18,
    // NOTE:
    //   Add new operator before `OperatorTypeMax`
    //   And DO NOT recorder or delete previous types
    //   Also add corresponding name in below `OperatorTypeName`
    OperatorTypeMax,
};

const char* OperatorTypeName(OperatorType type);

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_OPERATOR_TYPE_H_
