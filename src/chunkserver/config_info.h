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
 * Created Date: 19-2-28
 * Author: wudemiao
 */

#ifndef SRC_CHUNKSERVER_CONFIG_INFO_H_
#define SRC_CHUNKSERVER_CONFIG_INFO_H_

#include <string>
#include <memory>

#include "src/fs/local_filesystem.h"
#include "src/chunkserver/trash.h"
#include "src/chunkserver/inflight_throttle.h"
#include "src/chunkserver/concurrent_apply/concurrent_apply.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/common/authenticator.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::chunkserver::concurrent::ConcurrentApplyModule;

class FilePool;
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

    // If true, read requests will be invoked in current lease leader node.
    // If false, all requests will propose to raft (log read).
    // Default: true
    bool enbaleLeaseRead;

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
    // WAL segment file size
    uint32_t maxWalSegmentSize;
    // chunk文件的page大小
    uint32_t metaPageSize;
    // alignment for I/O request
    uint32_t blockSize;
    // clone chunk的location长度限制
    uint32_t locationLimit;

    // 并发模块
    ConcurrentApplyModule *concurrentapply;
    // Chunk file池子
    std::shared_ptr<FilePool> chunkFilePool;
    // WAL file pool
    std::shared_ptr<FilePool> walFilePool;
    // 文件系统适配层
    std::shared_ptr<LocalFileSystem> localFileSystem;
    // 回收站, 心跳模块判断该chunkserver不在copyset配置组时，
    // 通知copysetManager将copyset目录移动至回收站
    // 一段时间后实际回收物理空间
    std::shared_ptr<Trash> trash;

    // snapshot流控
    scoped_refptr<SnapshotThrottle> *snapshotThrottle;

    // 限制chunkserver启动时copyset并发恢复加载的数量,为0表示不限制
    uint32_t loadConcurrency = 0;
    // chunkserver sync_thread_pool number of threads.
    uint32_t syncConcurrency = 20;
    // copyset trigger sync timeout
    uint32_t syncTriggerSeconds = 25;
    // 检查copyset是否加载完成出现异常时的最大重试次数
    // 可能的异常：1.当前大多数副本还没起来；2.网络问题等导致无法获取leader
    // 3.其他的原因导致无法获取到leader的committed index
    uint32_t checkRetryTimes = 3;
    // 当前peer的applied_index与leader上的committed_index差距小于该值
    // 则判定copyset已经加载完成
    uint32_t finishLoadMargin = 2000;
    // 循环判定copyset是否加载完成的内部睡眠时间
    uint32_t checkLoadMarginIntervalMs = 1000;

    // enable O_DSYNC when open chunkfile
    bool enableOdsyncWhenOpenChunkFile = false;
    // syncChunkLimit default limit
    uint64_t syncChunkLimit = 2 * 1024 * 1024;
    // syncHighChunkLimit default limit = 64k
    uint64_t syncThreshold = 64 * 1024;
    // check syncing interval
    uint32_t checkSyncingIntervalMs = 500u;

    CopysetNodeOptions();
};

/**
 * ChunkServiceManager 的依赖项
 */
struct ChunkServiceOptions {
    CopysetNodeManager *copysetNodeManager;
    CloneManager *cloneManager;
    std::shared_ptr<InflightThrottle> inflightThrottle;
};

inline CopysetNodeOptions::CopysetNodeOptions()
    : electionTimeoutMs(1000),
      snapshotIntervalS(3600),
      enbaleLeaseRead(true),
      catchupMargin(1000),
      usercodeInPthread(false),
      logUri("/log"),
      raftMetaUri("/raft_meta"),
      raftSnapshotUri("/raft_snapshot"),
      chunkDataUri("/data"),
      chunkSnapshotUri("/snapshot"),
      recyclerUri("/recycler"),
      port(8200),
      maxChunkSize(16 * 1024 * 1024),
      metaPageSize(4096),
      blockSize(4096),
      concurrentapply(nullptr),
      chunkFilePool(nullptr),
      walFilePool(nullptr),
      localFileSystem(nullptr),
      snapshotThrottle(nullptr) {}

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CONFIG_INFO_H_
