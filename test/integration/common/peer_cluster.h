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
 * Created Date: 19-05-27
 * Author: wudemiao
 */

#ifndef TEST_INTEGRATION_COMMON_PEER_CLUSTER_H_
#define TEST_INTEGRATION_COMMON_PEER_CLUSTER_H_

#include <butil/status.h>
#include <gtest/gtest.h>
#include <unistd.h>
#include <braft/cli.h>

#include <string>
#include <vector>
#include <set>
#include <map>
#include <unordered_map>
#include <memory>

#include "src/chunkserver/datastore/file_pool.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/copyset_node.h"
#include "proto/common.pb.h"

namespace curve {
namespace chunkserver {

using curve::common::Peer;

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
    PeerNode() : pid(0), state(PeerNodeState::EXIT) {}
    // Peer对应的进程id
    pid_t pid;
    // Peer
    Peer peer;
    // copyset的集群配置
    Configuration conf;
    // PeerNode的状态
    PeerNodeState state;
};

/**
 * 封装模拟cluster测试相关的接口
 */
class PeerCluster {
 public:
    PeerCluster(const std::string &clusterName,
                const LogicPoolID logicPoolID,
                const CopysetID copysetID,
                const std::vector<Peer> &peers,
                std::vector<char **> params,
                std::map<int, int> paramsIndexs);
    virtual ~PeerCluster() { StopAllPeers(); }

 public:
    /**
     * 启动一个 Peer
     * @param peer
     * @param empty初始化配置是否为空
     * @return 0，成功；-1，失败
     */
    int StartPeer(const Peer &peer,
                  int id,
                  const bool empty = false);
    /**
     * 关闭一个peer，使用SIGINT
     * @param peer
     * @return 0 成功；-1 失败
     */
    int ShutdownPeer(const Peer &peer);


    /**
     * hang住一个peer，使用SIGSTOP
     * @param peer
     * @return 0成功；-1失败
     */
    int HangPeer(const Peer &peer);
    /**
    * 恢复hang住的peer，使用SIGCONT
    * @param peer
    * @return 0：成功，-1 失败
    */
    int SignalPeer(const Peer &peer);
    /**
     * 反复重试直到等到新的leader产生
     * @param leaderId出参，返回leader id
     * @return 0，成功；-1 失败
     */
    int WaitLeader(Peer *leaderPeer);

    /**
     * Stop所有的peer
     * @return 0，成功；-1 失败
     */
    int StopAllPeers();

 public:
    /* 返回集群当前的配置 */
    const Configuration CopysetConf() const;

    const LogicPoolID GetLogicPoolId() const {return logicPoolID_;}

    const CopysetID GetCopysetId() const {return copysetID_;}

    void SetWorkingCopyset(CopysetID copysetID) {copysetID_ = copysetID;}

    /* 修改 PeerNode 配置相关的接口，单位: s */
    int SetsnapshotIntervalS(int snapshotIntervalS);
    int SetElectionTimeoutMs(int electionTimeoutMs);

    static int StartPeerNode(int id, char *arg[]);

    static int PeerToId(const Peer &peer);

    static int GetFollwerPeers(const std::vector<Peer>& peers,
                               Peer leader,
                               std::vector<Peer> *followers);

 public:
    /**
     * 返回执行peer的copyset路径with protocol, ex: local://./127.0.0.1:9101:0
     */
    static const std::string CopysetDirWithProtocol(const Peer &peer);

    /**
     * 返回执行peer的copyset路径without protocol, ex: ./127.0.0.1:9101:0
     */
    static const std::string CopysetDirWithoutProtocol(const Peer &peer);

    /**
     * remove peer's copyset dir's cmd
     */
    static const std::string RemoveCopysetDirCmd(const Peer &peer);

    static const std::string RemoveCopysetLogDirCmd(const Peer &peer,
                                                    LogicPoolID logicPoolID,
                                                    CopysetID copysetID);

    static int CreateCopyset(LogicPoolID logicPoolID,
                             CopysetID copysetID,
                             Peer peer,
                             const std::vector<Peer>& peers);

 private:
    // 集群名字
    std::string             clusterName_;
    // 集群的peer集合
    std::vector<Peer>       peers_;
    // peer集合的映射map
    std::unordered_map<std::string, std::unique_ptr<PeerNode>> peersMap_;

    // 快照间隔
    int                     snapshotIntervalS_;
    // 选举超时时间
    int                     electionTimeoutMs_;
    // 集群成员配置
    Configuration           conf_;

    // 逻辑池id
    LogicPoolID             logicPoolID_;
    // 复制组id
    CopysetID               copysetID_;
    // chunkserver id
    static ChunkServerID    chunkServerId_;
    // 文件系统适配层
    static std::shared_ptr<LocalFileSystem> fs_;

    // chunkserver启动传入参数的映射关系(chunkserver id: params_'s index)
    std::map<int, int> paramsIndexs_;
    // chunkserver启动需要传递的参数列表
    std::vector<char **> params_;
};

/**
 * 正常 I/O 验证，先写进去，再读出来验证
 * @param leaderId      主的 id
 * @param logicPoolId   逻辑池 id
 * @param copysetId 复制组 id
 * @param chunkId   chunk id
 * @param length    每次 IO 的 length
 * @param fillCh    每次 IO 填充的字符
 * @param loop      重复发起 IO 的次数
 * @param sn        本次写入的版本号
 */
void WriteThenReadVerify(Peer leaderPeer,
                         LogicPoolID logicPoolId,
                         CopysetID copysetId,
                         ChunkID chunkId,
                         int length,
                         char fillCh,
                         int loop,
                         uint64_t sn = 1);

/**
 * 正常 I/O 验证，read 数据验证
 * @param leaderId      主的 id
 * @param logicPoolId   逻辑池 id
 * @param copysetId 复制组 id
 * @param chunkId   chunk id
 * @param length    每次 IO 的 length
 * @param fillCh    每次 IO 填充的字符
 * @param loop      重复发起 IO 的次数
 */
void ReadVerify(Peer leaderPeer,
                LogicPoolID logicPoolId,
                CopysetID copysetId,
                ChunkID chunkId,
                int length,
                char fillCh,
                int loop);

/**
 * 读chunk的snapshot进行验证
 * @param leaderId      主的 id
 * @param logicPoolId   逻辑池 id
 * @param copysetId 复制组 id
 * @param chunkId   chunk id
 * @param length    每次 IO 的 length
 * @param fillCh    每次 IO 填充的字符
 * @param loop      重复发起 IO 的次数
 */
void ReadSnapshotVerify(Peer leaderPeer,
                        LogicPoolID logicPoolId,
                        CopysetID copysetId,
                        ChunkID chunkId,
                        int length,
                        char fillCh,
                        int loop);

/**
 * 删除chunk的snapshot进行验证
 * @param leaderId      主的 id
 * @param logicPoolId   逻辑池 id
 * @param copysetId     复制组 id
 * @param chunkId       chunk id
 * @param csn           corrected sn
 */
void DeleteSnapshotVerify(Peer leaderPeer,
                          LogicPoolID logicPoolId,
                          CopysetID copysetId,
                          ChunkID chunkId,
                          uint64_t csn);

/**
 * 异常I/O验证，read数据不符合预期
 * @param leaderId      主的 id
 * @param logicPoolId   逻辑池 id
 * @param copysetId 复制组 id
 * @param chunkId   chunk id
 * @param length    每次 IO 的 length
 * @param fillCh    每次 IO 填充的字符
 * @param loop      重复发起 IO 的次数
 */
void ReadNotVerify(Peer leaderPeer,
                   LogicPoolID logicPoolId,
                   CopysetID copysetId,
                   ChunkID chunkId,
                   int length,
                   char fillCh,
                   int loop);

/**
 * 通过read验证可用性
 * @param leaderId      主的 id
 * @param logicPoolId   逻辑池 id
 * @param copysetId 复制组 id
 * @param chunkId   chunk id
 * @param length    每次 IO 的 length
 * @param fillCh    每次 IO 填充的字符
 * @param loop      重复发起 IO 的次数
 */
void ReadVerifyNotAvailable(Peer leaderPeer,
                            LogicPoolID logicPoolId,
                            CopysetID copysetId,
                            ChunkID chunkId,
                            int length,
                            char fillCh,
                            int loop);

/**
 * 通过write验证可用性
 * @param leaderId      主的 id
 * @param logicPoolId   逻辑池 id
 * @param copysetId 复制组 id
 * @param chunkId   chunk id
 * @param length    每次 IO 的 length
 * @param fillCh    每次 IO 填充的字符
 * @param loop      重复发起 IO 的次数
 */
void WriteVerifyNotAvailable(Peer leaderPeer,
                             LogicPoolID logicPoolId,
                             CopysetID copysetId,
                             ChunkID chunkId,
                             int length,
                             char fillCh,
                             int loop);

/**
 * 验证几个副本的copyset status是否一致
 * @param peerIds: 待验证的peers
 * @param logicPoolID: 逻辑池id
 * @param copysetId: 复制组id
 */
void CopysetStatusVerify(const std::vector<Peer> &peers,
                         LogicPoolID logicPoolID,
                         CopysetID copysetId,
                         uint64_t expectEpoch = 0);

/**
 * transfer leader，并且预期能够成功
 * @param cluster: 集群的指针
 * @param targetLeader: 期望tranfer的目标节点
 * @param opt: tranfer 请求使用的 clioption
 */
void TransferLeaderAssertSuccess(PeerCluster *cluster,
                                 const Peer &targetLeader,
                                 braft::cli::CliOptions opt);

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_INTEGRATION_COMMON_PEER_CLUSTER_H_
