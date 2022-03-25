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

#ifndef CURVEFS_SRC_METASERVER_COMMON_OPERATOR_TYPE_H_
#define CURVEFS_SRC_METASERVER_COMMON_OPERATOR_TYPE_H_

#include <cstdint>

namespace curvefs {
namespace metaserver {

enum class OperatorType : uint32_t {
    GetDentry,
    ListDentry,
    CreateDentry,
    DeleteDentry,
    GetInode,
    BatchGetInodeAttr,
    BatchGetXAttr,
    CreateInode,
    UpdateInode,
    DeleteInode,
    CreateRootInode,
    CreatePartition,
    DeletePartition,
    PrepareRenameTx,
    GetOrModifyS3ChunkInfo,
    /** Add new operator before `OperatorTypeMax` **/
    OperatorTypeMax,
};

const char* OperatorTypeName(OperatorType type);

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COMMON_OPERATOR_TYPE_H_
