/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_COPYSET_NODE_H
#define CURVE_CHUNKSERVER_COPYSET_NODE_H

#include <butil/memory/ref_counted.h>

#include <string>
#include <vector>
#include <climits>

#include "src/chunkserver/chunkserverStorage/chunkserver_datastore.h"
#include "proto/chunk.pb.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

class CopysetNodeManager;

/**
 * copyset node 的配置选项
 * TODO(wudemiao): braft 中包含非常多的 FLAGS，前期主要考虑几个核心的 config，
 * 也就是 NodeOptions
 */
struct CopysetNodeOptions {
    /* Raft 复制组的配置选项，继承自 braft */
    // follower to candidate timeout, 单位 ms
    // from the leader in |election_timeout_ms| milliseconds
    // Default: 1000 (1s)
    int electionTimeoutMs;
    // A snapshot saving would be triggered every |snapshot_interval_s| seconds
    // if this was reset as a positive number
    // If |snapshot_interval_s| <= 0, the time based snapshot would be disabled.
    //
    // Default: 3600,单位 s (1 hour)
    int snapshotIntervalS;
    // We will regard a adding peer as caught up if the margin between the
    // last_log_index of this peer and the last_log_index of leader is less than
    // |catchup_margin|
    //
    // Default: 1000
    int catchupMargin;

    // Run the user callbacks and user closures in pthread rather than bthread
    //
    // Default: false
    bool usercodeInPthread;

    // If true, RPCs through raft_cli will be denied.
    // Default: false
    bool disableCli;

    // for all uri: ${protocol}://${绝对或者相对路径}
    // posix: local
    // bluestore: bluestore

    // raft log uri
    //
    // Default: log
    std::string logUri;

    // Raft meta uri
    //
    // Default: meta
    std::string raftMetaUri;

    // snapshot uri
    //
    // Default: snapshot
    std::string raftSnapshotUri;

    /* copysetNode 独有的配置 */
    // chunk data 目录
    //
    // Default : data
    std::string chunkDataUri;
    // chunk snapshot 目录
    //
    // Default: snapshot
    std::string chunkSnapshotUri;

    std::string ip;
    uint32_t port;
    uint32_t maxChunkSize;

    // The CopysetNodeManager that it belongs to
    CopysetNodeManager *copysetNodeManager;

    CopysetNodeOptions();
    CopysetNodeOptions(const CopysetNodeOptions &copysetNodeOptions);
    CopysetNodeOptions &operator=(const CopysetNodeOptions &copysetNodeOptions);
};

class CopysetNode : public braft::StateMachine,
                    public std::enable_shared_from_this<CopysetNode> {
 public:
    CopysetNode(const LogicPoolID &logicPoolId,
                const CopysetID &copysetId,
                const Configuration &initConf);

    virtual ~CopysetNode();

    /* 初始化 copyset node 配置，并将 CopysetNode 加入 Manager 管理 */
    int Init(const CopysetNodeOptions &options);
    int Run();
    /* shutdown */
    void Fini();

    /* 将chunk读写流request打包成task并apply给raft node走一致性协议处理 */
    void DeleteChunk(RpcController *controller,
                     const ChunkRequest *request,
                     ChunkResponse *response,
                     google::protobuf::Closure *done);
    void ReadChunk(RpcController *controller,
                   const ChunkRequest *request,
                   ChunkResponse *response,
                   google::protobuf::Closure *done);
    void WriteChunk(RpcController *controller,
                    const ChunkRequest *request,
                    ChunkResponse *response,
                    Closure *done);

    /* chunk snapshot 创建和删除，也是通过 raft 一致性协议来完成创建 */
    void CreateChunkSnapshot(RpcController *controller,
                             const ChunkSnapshotRequest *request,
                             ChunkSnapshotResponse *response,
                             Closure *done);
    void DeleteChunkSnapshot(RpcController *controller,
                             const ChunkSnapshotRequest *request,
                             ChunkSnapshotResponse *response,
                             Closure *done);
    void ReadChunkSnapshot(RpcController *controller,
                           const ChunkSnapshotRequest *request,
                           ChunkSnapshotResponse *response,
                           Closure *done);

    /* 不是 leader，转发请求 */
    void RedirectChunkRequest(ChunkResponse *response);
    void RedirectChunkSnapshotRequest(ChunkSnapshotResponse *response);

 public:
    /* 下面的接口都是继承 StateMachine 实现的接口 */
    void on_apply(::braft::Iterator &iter);     //NOLINT
    void on_shutdown();
    /* raft snapshot 相关的接口,仅仅保存 raft snapshot meta */
    void on_snapshot_save(::braft::SnapshotWriter *writer,
                          ::braft::Closure *done);
    /**
     *  load 日志有两种情况：
     *  1. Follower 节点 Install snapshot 追赶 leader，这个时候 snapshot 目录下面有
     *  chunk 数据和 snapshot 数据
     *  2. 节点重启，会执行 snapshot load，然后回放日志，这个时候 snapshot 目录下面没
     *  有数据，什么都不用做
     */
    int on_snapshot_load(::braft::SnapshotReader *reader);
    void on_leader_start(int64_t term);
    void on_leader_stop(const butil::Status &status);
    void on_error(const ::braft::Error &e);
    void on_configuration_committed(const ::braft::Configuration &conf);
    void on_stop_following(const ::braft::LeaderChangeContext &ctx);
    void on_start_following(const ::braft::LeaderChangeContext &ctx);

 private:
    void ApplyChunkRequest(RpcController *controller,
                           const ChunkRequest *request,
                           ChunkResponse *response,
                           Closure *done);
    void ApplyChunkSnapshotRequest(RpcController *controller,
                                   const ChunkSnapshotRequest *request,
                                   ChunkSnapshotResponse *response,
                                   Closure *done);

 private:
    CopysetNodeManager *copysetNodeManager_;
    LogicPoolID logicPoolId_;
    CopysetID copysetId_;
    Configuration initConf_;
    PeerId peerId_;
    NodeOptions nodeOptions_;
    std::shared_ptr<Node> raftNode_;
    /* chunk 数据的文件系统协议 */
    std::string filesystemProtocol_;
    /* chunk file 的绝对目录 */
    std::string chunkDataApath_;
    /* chunk file 的相对目录 */
    std::string chunkDataRpath_;
    /* 预留，暂时没有使用 */
    std::string chunkSnapshotUri_;
    scoped_refptr<FileSystemAdaptor> fs_;
    friend class ChunkOpRequest;
    std::unique_ptr<CSDataStore> dataStore_;
    std::atomic<int64_t> leaderTerm_;
};

/**
 *  group id 格式如下：
 *  |            group id           |
 *  |     32         |      32      |
 *  | logic pool id  |  copyset id  |
 */
inline GroupId ToGroupId(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId) {
    uint64_t groupId = (static_cast<uint64_t>(logicPoolId) << 32) | copysetId;
    return std::to_string(groupId);
}

/* 格式输出 group id 的 字符串 (logicPoolId, copysetId) */
inline std::string ToGroupIdString(const LogicPoolID &logicPoolId,
                                   const CopysetID &copysetId) {
    std::string groupIdString;
    groupIdString.append("(");
    groupIdString.append(std::to_string(logicPoolId));
    groupIdString.append(", ");
    groupIdString.append(std::to_string(copysetId));
    groupIdString.append(")");
    return groupIdString;
}

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_COPYSET_NODE_H
