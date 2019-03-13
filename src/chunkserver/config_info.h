/*
 * Project: curve
 * Created Date: 19-2-28
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_CONFIG_INFO_H_
#define SRC_CHUNKSERVER_CONFIG_INFO_H_

#include <string>

#include "src/fs/local_filesystem.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;

class ConcurrentApplyModule;
class ChunkfilePool;
class CopysetNodeManager;
class CloneManager;

/**
 * copyset node的配置选项
 */
struct CopysetNodeOptions {
    // follower to candidate 超时时间，单位ms，默认是1000ms
    int electionTimeoutMs;

    // 定期打快照的时间间隔，默认3600s，也就是1小时
    int snapshotIntervalS;

    // 如果follower和leader日志相差超过catchupMargin，
    // 就会执行install snapshot进行恢复，默认: 1000
    int catchupMargin;

    // 是否开启pthread执行用户代码，默认false
    bool usercodeInPthread;

    // 所有uri个格式: ${protocol}://${绝对或者相对路径}
    // eg:
    // posix: local
    // bluestore: bluestore

    // raft log uri, 默认raft_log
    std::string logUri;

    // raft meta uri, 默认raft_meta
    std::string raftMetaUri;

    // raft snapshot uri，默认raft_snpashot
    std::string raftSnapshotUri;

    // chunk data uri，默认data
    std::string chunkDataUri;

    // chunk snapshot uri，默认snapshot
    std::string chunkSnapshotUri;

    // copyset data recycling uri，默认recycler
    std::string recyclerUri;

    std::string ip;
    uint32_t port;
    // chunk文件的大小
    uint32_t maxChunkSize;
    // chunk文件的page大小
    uint32_t pageSize;

    // 并发模块
    ConcurrentApplyModule *concurrentapply;
    // Chunk file池子
    std::shared_ptr<ChunkfilePool> chunkfilePool;
    // 文件系统适配层
    std::shared_ptr<LocalFileSystem> localFileSystem;

    CopysetNodeOptions();
};

/**
 * ChunkServiceManager 的依赖项
 */
struct ChunkServiceOptions {
    CopysetNodeManager *copysetNodeManager;
    CloneManager *cloneManager;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CONFIG_INFO_H_
