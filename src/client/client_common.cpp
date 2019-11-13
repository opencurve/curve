/*
 * Project: curve
 * Create Data: 2019年11月13日
 * Author: wuhanqing
 * Copyright (c) 2018 netease
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

