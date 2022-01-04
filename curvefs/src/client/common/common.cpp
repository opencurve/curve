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

#include "curvefs/src/client/common/common.h"
namespace curvefs {
namespace client {
namespace common {

std::ostream &operator<<(std::ostream &os, MetaServerOpType optype) {
    switch (optype) {
    case MetaServerOpType::GetDentry:
        os << "GetDentry";
        break;
    case MetaServerOpType::ListDentry:
        os << "ListDentry";
        break;
    case MetaServerOpType::CreateDentry:
        os << "CreateDentry";
        break;
    case MetaServerOpType::DeleteDentry:
        os << "DeleteDentry";
        break;
    case MetaServerOpType::GetInode:
        os << "GetInode";
        break;
    case MetaServerOpType::UpdateInode:
        os << "UpdateInode";
        break;
    case MetaServerOpType::CreateInode:
        os << "CreateInode";
        break;
    case MetaServerOpType::DeleteInode:
        os << "DeleteInode";
        break;
    case MetaServerOpType::AppendS3ChunkInfo:
        os << "AppendS3ChunkInfo";
        break;
    default:
        os << "Unknow opType";
    }
    return os;
}

}  // namespace common
}  // namespace client
}  // namespace curvefs
