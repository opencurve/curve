/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_REQUEST_H
#define CURVE_CHUNKSERVER_REQUEST_H

#include <google/protobuf/message.h>
#include <butil/iobuf.h>

#include <memory>

#include "proto/chunk.pb.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

class QosRequest {
 public:
    QosRequest() {}
    virtual ~QosRequest() {}
    virtual void Schedule() = 0;
};

class ChunkOpRequest : public QosRequest,
                       public std::enable_shared_from_this<ChunkOpRequest> {
 public:
    ChunkOpRequest(CopysetNodeManager *copysetNodeManager,
                   RpcController *cntl,
                   const ChunkRequest *request,
                   ChunkResponse *response,
                   Closure *done) :
        copysetNodeManager_(copysetNodeManager),
        cntl_(cntl),
        request_(request),
        response_(response),
        done_(done) {}
    /* 它的成员的生命周期都归rpc框架管，所以自己不会主动去析构 */
    virtual ~ChunkOpRequest() {}

    void Schedule() override;
    void Process();

 private:
    CopysetNodeManager  *copysetNodeManager_;
    RpcController       *cntl_;
    const ChunkRequest  *request_;
    ChunkResponse       *response_;
    Closure             *done_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_REQUEST_H
