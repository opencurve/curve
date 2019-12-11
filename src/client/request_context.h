/*
 * Project: curve
 * File Created: Monday, 17th September 2018 4:19:43 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef SRC_CLIENT_REQUEST_CONTEXT_H_
#define SRC_CLIENT_REQUEST_CONTEXT_H_

#include <atomic>
#include <string>

#include "src/client/client_common.h"
#include "src/client/request_closure.h"

namespace curve {
namespace client {
class RequestContext {
 public:
    RequestContext();
    ~RequestContext() = default;
    bool Init();
    void UnInit();

    // chunk的ID信息，sender在发送rpc的时候需要附带其ID信息
    ChunkIDInfo         idinfo_;

    // 用户IO被拆分之后，其小IO有自己的offset和length
    off_t               offset_;
    OpType              optype_;
    size_t              rawlength_;

    // 当前IO的数据，读请求时数据在readbuffer，写请求在writebuffer
    char*               readBuffer_;
    const char*         writeBuffer_;

    // 因为RPC都是异步发送，因此在一个Request结束时，RPC回调调用当前的done
    // 来告知当前的request结束了
    RequestClosure*     done_;

    // request的版本信息
    uint64_t            seq_;
    // appliedindex_表示当前IO是否走chunkserver端的raft协议，为0的时候走raft
    uint64_t            appliedindex_;

    // 这个对应的GetChunkInfo的出参
    ChunkInfoDetail*    chunkinfodetail_;

    // clone chunk请求需要携带源chunk的location及所需要创建的chunk的大小
    uint32_t            chunksize_;
    std::string         location_;
    // create clone chunk时候用于修改chunk的correctedSn
    uint64_t            correctedSeq_;

    // 当前request context id
    uint64_t            id_;

    // request context id生成器
    static std::atomic<uint64_t> reqCtxID_;
};

inline std::ostream& operator<<(std::ostream& os,
                                const RequestContext& reqCtx) {
    os << "logicpool id = " << reqCtx.idinfo_.lpid_
       << ", copyset id = " << reqCtx.idinfo_.cpid_
       << ", chunk id = " << reqCtx.idinfo_.cid_
       << ", offset = " << reqCtx.offset_
       << ", length = " << reqCtx.rawlength_
       << ", sn = " << reqCtx.seq_;

    return os;
}

}  // namespace client
}  // namespace curve
#endif  // SRC_CLIENT_REQUEST_CONTEXT_H_
