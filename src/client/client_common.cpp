/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Create Data: 2019年11月13日
 * Author: wuhanqing
 */

#include "src/client/client_common.h"

namespace curve {
namespace client {

const char* OpTypeToString(OpType optype) {
    switch (optype) {
    case OpType::READ:
        return "Read";
    case OpType::WRITE:
        return "Write";
    case OpType::READ_SNAP:
        return "ReadSnapshot";
    case OpType::DELETE_SNAP:
        return "DeleteSnapshot";
    case OpType::CREATE_CLONE:
        return "CreateCloneChunk";
    case OpType::RECOVER_CHUNK:
        return "RecoverChunk";
    case OpType::GET_CHUNK_INFO:
        return "GetChunkInfo";
    case OpType::UNKNOWN:
    default:
        return "Unknown";
    }
}

}  // namespace client
}  // namespace curve

