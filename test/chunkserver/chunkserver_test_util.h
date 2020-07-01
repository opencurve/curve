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
 * Created Date: 18-11-25
 * Author: wudemiao
 */

#ifndef TEST_CHUNKSERVER_CHUNKSERVER_TEST_UTIL_H_
#define TEST_CHUNKSERVER_CHUNKSERVER_TEST_UTIL_H_

#include <butil/status.h>
#include <unistd.h>

#include <string>
#include <vector>
#include <set>
#include <memory>
#include <unordered_map>

#include "src/chunkserver/datastore/chunkfile_pool.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/copyset_node.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;

std::string Exec(const char *cmd);

/**
 * 当前chunkfilepool需要事先格式化，才能使用，此函数用于事先格式化chunkfilepool
 * @param fsptr:本文文件系统指针
 * @param chunkfileSize:chunk文件的大小
 * @param metaPageSize:chunk文件的meta page大小
 * @param poolpath:文件池的路径，例如./chunkfilepool/
 * @param metaPath:meta文件路径，例如./chunkfilepool/chunkfilepool.meta
 * @return 初始化成功返回ChunkfilePool指针，否则返回null
 */
std::shared_ptr<ChunkfilePool> InitChunkfilePool(std::shared_ptr<LocalFileSystem> fsptr,    //NOLINT
                                                 int chunkfileCount,
                                                 int chunkfileSize,
                                                 int metaPageSize,
                                                 std::string poolpath,
                                                 std::string metaPath);

int StartChunkserver(const char *ip,
                     int port,
                     const char *copysetdir,
                     const char *confs,
                     const int snapshotInterval,
                     const int electionTimeoutMs);

butil::Status WaitLeader(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId,
                         const Configuration &conf,
                         PeerId *leaderId,
                         int electionTimeoutMs);

/**
 * PeerNode 状态
 * 1. exit：未启动，或者被关闭
 * 2. running：正在运行
 * 3. stop：hang 住了
 */
enum class PeerNodeState {
    EXIT = 0,       // 退出
    RUNNING = 1,    // 正在运行
    STOP = 2,       // hang住
};

/**
 * 一个 ChunkServer 进程，包含某个 Copyset 的某个副本
 */
struct PeerNode {
    PeerNode() : pid(0), options(), state(PeerNodeState::EXIT) {}
    // Peer对应的进程id
    pid_t pid;
    // Peer的地址
    PeerId peerId;
    // copyset的集群配置
    Configuration conf;
    // copyset的基本配置
    CopysetNodeOptions options;
    // PeerNode的状态
    PeerNodeState state;
};

/**
 * 封装模拟 cluster 测试相关的接口
 */
class TestCluster {
 public:
    TestCluster(const std::string &clusterName,
                const LogicPoolID logicPoolID,
                const CopysetID copysetID,
                const std::vector<PeerId> &peers);
    virtual ~TestCluster() { StopAllPeers(); }

 public:
    /**
     * 启动一个 Peer
     * @param peerId
     * @param empty 初始化配置是否为空
     * @param: get_chunk_from_pool是否从chunkfilepool获取chunk
     * @param: create_chunkfilepool是否创建chunkfilepool，重启的情况下不需要
     * @return 0：成功，-1 失败
     */
    int StartPeer(const PeerId &peerId,
                  const bool empty = false,
                  bool get_chunk_from_pool = false,
                  bool create_chunkfilepool = true);
    /**
     * 关闭一个 peer，使用 SIGINT
     * @param peerId
     * @return 0：成功，-1 失败
     */
    int ShutdownPeer(const PeerId &peerId);


    /**
     * hang 住一个 peer，使用 SIGSTOP
     * @param peerId
     * @return 0：成功，-1 失败
     */
    int StopPeer(const PeerId &peerId);
    /**
    * 恢复 hang 住的 peer，使用 SIGCONT
    * @param peerId
    * @return 0：成功，-1 失败
    */
    int ContPeer(const PeerId &peerId);
    /**
     * 反复重试直到等到新的 leader 产生
     * @param leaderId 出参，返回 leader id
     * @return 0：成功，-1 失败
     */
    int WaitLeader(PeerId *leaderId);

    /**
     * Stop 所有的 peer
     * @return 0：成功，-1 失败
     */
    int StopAllPeers();

 public:
    /* 返回集群当前的配置 */
    const Configuration CopysetConf() const;

    /* 修改 PeerNode 配置相关的接口，单位: s */
    int SetsnapshotIntervalS(int snapshotIntervalS);
    int SetElectionTimeoutMs(int electionTimeoutMs);
    int SetCatchupMargin(int catchupMargin);

    static int StartPeerNode(CopysetNodeOptions options,
                              const Configuration conf,
                              bool from_chunkfile_pool = false,
                              bool create_chunkfilepool = true);

 public:
    /**
    * 返回执行 peer 的 copyset 路径 with protocol, ex: local://./127.0.0.1:9101:0
    */
    static const std::string CopysetDirWithProtocol(const PeerId &peerId);
    /**
     * 返回执行 peer 的 copyset 路径 without protocol, ex: ./127.0.0.1:9101:0
     */
    static const std::string CopysetDirWithoutProtocol(const PeerId &peerId);
    /**
     * remove peer's copyset dir's cmd
     */
    static const std::string RemoveCopysetDirCmd(const PeerId &peerid);

 private:
    // 集群名字
    std::string         clusterName_;
    // 集群的peer集合
    std::set<PeerId>    peers_;
    // peer集合的映射map
    std::unordered_map<std::string, std::unique_ptr<PeerNode>> peersMap_;

    // 快照间隔
    int snapshotIntervalS_;
    // 选举超时时间
    int electionTimeoutMs_;
    // catchup margin配置
    int catchupMargin_;
    // 集群成员配置
    Configuration conf_;

    // 逻辑池id
    static LogicPoolID  logicPoolID_;
    // 复制组id
    static CopysetID    copysetID_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_CHUNKSERVER_TEST_UTIL_H_
