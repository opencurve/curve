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
 * File Created: Monday, 17th September 2018 4:19:43 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_REQUEST_CONTEXT_H_
#define SRC_CLIENT_REQUEST_CONTEXT_H_

#include <butil/iobuf.h>

#include <atomic>
#include <string>
#include <vector>

#include "src/client/client_common.h"
#include "src/client/request_closure.h"
#include "include/curve_compiler_specific.h"

namespace curve {
namespace client {

struct RequestSourceInfo {
    std::string cloneFileSource;
    uint64_t cloneFileOffset = 0;
    bool valid = false;

    RequestSourceInfo() = default;
    RequestSourceInfo(const std::string& source, uint64_t offset)
        : cloneFileSource(source), cloneFileOffset(offset), valid(true) {}

    bool IsValid() const { return valid; }
};

inline std::ostream& operator<<(std::ostream& os,
                                const RequestSourceInfo& location) {
    if (location.IsValid()) {
        os << location.cloneFileSource << ":" << location.cloneFileOffset;
    } else {
        os << "empty";
    }

    return os;
}

struct CURVE_CACHELINE_ALIGNMENT RequestContext {
    RequestContext() : id_(GetNextRequestContextId()) {}

    ~RequestContext() = default;

    bool Init() {
         done_ = new (std::nothrow) RequestClosure(this);
         return done_ != nullptr;
    }

    void UnInit() {
        delete done_;
        done_ = nullptr;
    }

    // chunk的ID信息，sender在发送rpc的时候需要附带其ID信息
    ChunkIDInfo         idinfo_;

    // 用户IO被拆分之后，其小IO有自己的offset和length
    off_t               offset_ = 0;
    OpType              optype_ = OpType::UNKNOWN;
    size_t              rawlength_ = 0;

    // user's single io request will split into several requests
    // subIoIndex_ is an index of serveral requests
    uint32_t subIoIndex_ = 0;

    // read data of current request
    butil::IOBuf readData_;

    // write data of current request
    butil::IOBuf writeData_;

    // 因为RPC都是异步发送，因此在一个Request结束时，RPC回调调用当前的done
    // 来告知当前的request结束了
    RequestClosure*     done_ = nullptr;

    // file id
    uint64_t fileId_;
    // file epoch
    uint64_t epoch_;
    // request的版本信息
    uint64_t            seq_ = 0;
    // sequence nums of current existed snapshots,
    // required for multi-level snapshot
    std::vector<uint64_t> snaps_;
    // appliedindex_表示当前IO是否走chunkserver端的raft协议，为0的时候走raft
    uint64_t            appliedindex_ = 0;

    // 这个对应的GetChunkInfo的出参
    ChunkInfoDetail*    chunkinfodetail_ = nullptr;

    // clone chunk请求需要携带源chunk的location及所需要创建的chunk的大小
    uint32_t            chunksize_ = 0;
    std::string         location_;
    RequestSourceInfo   sourceInfo_;
    // create clone chunk时候用于修改chunk的correctedSn
    uint64_t            correctedSeq_ = 0;

    // 当前request context id
    uint64_t            id_ = 0;

    FileType            filetype_;
    std::vector<CloneInfo> cloneChain_;
    uint64_t            originFileId_;
    uint64_t            chunkIndex_;

    uint32_t version_;

    static RequestContext* NewInitedRequestContext() {
        RequestContext* ctx = new (std::nothrow) RequestContext();
        if (ctx && ctx->Init()) {
            return ctx;
        } else {
            LOG(ERROR) << "Allocate or Init RequestContext Failed";
            delete ctx;
            return nullptr;
        }
    }

 private:
    static std::atomic<uint64_t> requestId;

    static uint64_t GetNextRequestContextId() {
        return requestId.fetch_add(1, std::memory_order_relaxed);
    }
};

inline std::ostream& operator<<(std::ostream& os,
                                const RequestContext& reqCtx) {
    os << "logicpool id = " << reqCtx.idinfo_.lpid_
       << ", copyset id = " << reqCtx.idinfo_.cpid_
       << ", chunk id = " << reqCtx.idinfo_.cid_
       << ", offset = " << reqCtx.offset_
       << ", length = " << reqCtx.rawlength_
       << ", sub-io index = " << reqCtx.subIoIndex_
       << ", sn = " << reqCtx.seq_
       << ", source info = " << reqCtx.sourceInfo_;

    return os;
}

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_REQUEST_CONTEXT_H_
