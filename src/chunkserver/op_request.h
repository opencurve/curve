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

enum class RequestType {
    CHUNK_OP = 0,            // chunk file
    CHUNK_SNAPSHOT_OP = 1,   // chunk snapshot
    UNKNOWN_OP = 2
};

class QosRequest {
 public:
    QosRequest() {}

    virtual ~QosRequest() {}

    virtual void Schedule() = 0;
};

// TODO(wudemiao): 还需要能一些 QoS 的参数和方法
class OpRequest : public QosRequest {
 public:
    OpRequest() {}

    virtual ~OpRequest() {}

    virtual void Process() = 0;

    virtual int OnApply(std::shared_ptr<CopysetNode> copysetNode) = 0;

    // 序列化这个 Op，为提交给 raft node 写入 WAL 做准备
    virtual int Encode(butil::IOBuf *data) = 0;
};

class ChunkOpRequest : public OpRequest,
                       public std::enable_shared_from_this<ChunkOpRequest> {
 public:
    ChunkOpRequest(CopysetNodeManager *copysetNodeManager,
                   ::google::protobuf::RpcController *cntl,
                   const ChunkRequest *request,
                   ChunkResponse *response,
                   ::google::protobuf::Closure *done) :
        copysetNodeManager_(copysetNodeManager),
        cntl_(cntl),
        request_(request),
        response_(response),
        done_(done) {}

    // 它的成员的生命周期都归rpc框架管，所以自己不会主动去析构
    virtual ~ChunkOpRequest() {}

    ::google::protobuf::Closure *GetClosure() {
        return done_;
    }

    const ChunkRequest *GetRequest() {
        return request_;
    }

    ChunkResponse *GetResponse() {
        return response_;
    }

    void Schedule() override;

    void Process() override;

    int OnApply(std::shared_ptr<CopysetNode> copysetNode) override;

    // Op 序列化
    // |                    data                   |
    // | op meta                      |   op data  |
    // | op type | op request length  | op request |
    // | 8 bit   |     32 bit         |  ....      |
    int Encode(butil::IOBuf *data) override;

    static void OnApply(std::shared_ptr<CopysetNode> copysetNode, butil::IOBuf *log);

 private:
    CopysetNodeManager *copysetNodeManager_;
    ::google::protobuf::RpcController *cntl_;
    const ChunkRequest *request_;
    ChunkResponse *response_;
    ::google::protobuf::Closure *done_;
};

class ChunkSnapshotOpRequest : public OpRequest,
                               public std::enable_shared_from_this<ChunkSnapshotOpRequest> {
 public:
    ChunkSnapshotOpRequest(CopysetNodeManager *copysetNodeManager,
                           ::google::protobuf::RpcController *cntl,
                           const ChunkSnapshotRequest *request,
                           ChunkSnapshotResponse *response,
                           ::google::protobuf::Closure *done) :
        copysetNodeManager_(copysetNodeManager),
        cntl_(cntl),
        request_(request),
        response_(response),
        done_(done) {}

    virtual ~ChunkSnapshotOpRequest() {}

    ::google::protobuf::Closure *GetClosure() {
        return done_;
    }

    const ChunkSnapshotRequest *GetRequest() {
        return request_;
    }

    ChunkSnapshotResponse *GetResponse() {
        return response_;
    }

    void Schedule() override;

    void Process() override;

    int OnApply(std::shared_ptr<CopysetNode> copysetNode) override;

    int Encode(butil::IOBuf *data) override;

    static void OnApply(std::shared_ptr<CopysetNode> copysetNode, butil::IOBuf *log);

 private:
    CopysetNodeManager *copysetNodeManager_;
    ::google::protobuf::RpcController *cntl_;
    const ChunkSnapshotRequest *request_;
    ChunkSnapshotResponse *response_;
    ::google::protobuf::Closure *done_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_REQUEST_H
