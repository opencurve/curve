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
#include <brpc/server.h>

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
#include "proto/topology.pb.h"

using ::curve::mds::topology::TopologyService;
using ::curve::mds::topology::ChunkServerRegistRequest;
using ::curve::mds::topology::ChunkServerRegistResponse;

namespace curve {
namespace chunkserver {

using curve::common::Peer;

/**
 * PeerNode status
 * 1. exit: Not started or closed
 * 2. running: Running
 * 3. stop: hang
 */
enum class PeerNodeState {
    EXIT = 0,       // Exit
    RUNNING = 1,    // Running
    STOP = 2,       // Hang Stay
};

/**
 * A ChunkServer process that contains a copy of a Copyset
 */
struct PeerNode {
    PeerNode() : pid(0), state(PeerNodeState::EXIT) {}
    // Process ID corresponding to Peer
    pid_t pid;
    // Peer
    Peer peer;
    // Cluster configuration for copyset
    Configuration conf;
    // Status of PeerNode
    PeerNodeState state;
};

class FakeTopologyService : public TopologyService {
    void RegistChunkServer(google::protobuf::RpcController* cntl_base,
                      const ChunkServerRegistRequest* request,
                      ChunkServerRegistResponse* response,
                      google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        response->set_statuscode(0);
        response->set_chunkserverid(request->chunkserverid());
        response->set_token(request->token());
    }
};

/**
 * Package simulation cluster testing related interfaces
 */
class PeerCluster {
 public:
    PeerCluster(const std::string &clusterName,
                const LogicPoolID logicPoolID,
                const CopysetID copysetID,
                const std::vector<Peer> &peers,
                std::vector<char **> params,
                std::map<int, int> paramsIndexs);
    virtual ~PeerCluster() {
        StopAllPeers();
        if (isFakeMdsStart_) {
            fakeMdsServer_.Stop(0);
            fakeMdsServer_.Join();
        }
    }

 public:
    /**
     * @brief start a fake topology service for register
     *
     * @return 0 for success, -1 for failed
     */
    int StartFakeTopoloyService(const std::string &listenAddr);

    /**
     * Start a Peer
     * @param peer
     * @param empty Is the initialization configuration empty
     * @return 0, successful- 1. Failure
     */
    int StartPeer(const Peer &peer,
                  int id,
                  const bool empty = false);
    /**
     * Close a peer and use SIGINT
     * @param peer
     * @return 0 successful, - 1 failed
     */
    int ShutdownPeer(const Peer &peer);


    /**
     * hang lives in a peer and uses SIGSTOP
     * @param peer
     * @return 0 successful, - 1 failed
     */
    int HangPeer(const Peer &peer);
    /**
    * Restore the peer where Hang lives and use SIGCONT
    * @param peer
    * @return 0: Success, -1 failed
    */
    int SignalPeer(const Peer &peer);
    /**
     * Try again and again until a new leader is generated
     * @param leaderPeer generates parameters and returns leader information
     * @return 0 successful, - 1 failed
     */
    int WaitLeader(Peer *leaderPeer);

    /**
     * confirm leader
     * @param: LogicPoolID logicalPool id
     * @param: copysetId copyset id
     * @param: leaderAddr leader address
     * @param: leader leader information
     * @return 0 successful, - 1 failed
     */
    int ConfirmLeader(const LogicPoolID &logicPoolId,
                        const CopysetID &copysetId,
                        const std::string& leaderAddr,
                        Peer *leader);


    /**
     * Stop all peers
     * @return 0 successful, - 1 failed
     */
    int StopAllPeers();

 public:
    /* Returns the current configuration of the cluster */
    Configuration CopysetConf() const;

    LogicPoolID GetLogicPoolId() const {return logicPoolID_;}

    CopysetID GetCopysetId() const {return copysetID_;}

    void SetWorkingCopyset(CopysetID copysetID) {copysetID_ = copysetID;}

    /* Modify the interface related to PeerNode configuration, unit: s */
    int SetsnapshotIntervalS(int snapshotIntervalS);
    int SetElectionTimeoutMs(int electionTimeoutMs);

    static int StartPeerNode(int id, char *arg[]);

    static int PeerToId(const Peer &peer);

    static int GetFollwerPeers(const std::vector<Peer>& peers,
                               Peer leader,
                               std::vector<Peer> *followers);

 public:
    /**
     * Returns the copyset path for executing peer with protocol, ex: local://./127.0.0.1:9101:0
     */
    static const std::string CopysetDirWithProtocol(const Peer &peer);

    /**
     * Returns the copyset path for executing peer without protocol, ex: ./127.0.0.1:9101:0
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
    // Cluster Name
    std::string             clusterName_;
    // The peer set of the cluster
    std::vector<Peer>       peers_;
    // Mapping Map of Peer Set
    std::unordered_map<std::string, std::unique_ptr<PeerNode>> peersMap_;

    // Snapshot interval
    int                     snapshotIntervalS_;
    // Election timeout
    int                     electionTimeoutMs_;
    // Cluster member configuration
    Configuration           conf_;

    // Logical Pool ID
    LogicPoolID             logicPoolID_;
    // Copy Group ID
    CopysetID               copysetID_;
    // chunkserver id
    static ChunkServerID    chunkServerId_;
    // File System Adaptation Layer
    static std::shared_ptr<LocalFileSystem> fs_;

    // chunkserver starts the mapping relationship of incoming parameters (chunkserver id: params_'s index)
    std::map<int, int> paramsIndexs_;
    // List of parameters to be passed for chunkserver startup
    std::vector<char **> params_;

    // fake mds server
    brpc::Server fakeMdsServer_;
    // fake topology service
    FakeTopologyService fakeTopologyService_;
    // is fake mds start
    bool isFakeMdsStart_;
};

/**
 * Normal I/O verification, write it in first, then read it out for verification
 * @param leaderId    Primary ID
 * @param logicPoolId Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId   chunk id
 * @param length    The length of each IO
 * @param fillCh    Characters filled in each IO
 * @param loop      The number of times repeatedly initiates IO
 * @param sn        The version number written this time
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
 * Normal I/O verification, read data verification
 * @param leaderId    Primary ID
 * @param logicPoolId Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId   chunk id
 * @param length    The length of each IO
 * @param fillCh    Characters filled in each IO
 * @param loop      The number of times repeatedly initiates IO
 */
void ReadVerify(Peer leaderPeer,
                LogicPoolID logicPoolId,
                CopysetID copysetId,
                ChunkID chunkId,
                int length,
                char fillCh,
                int loop);

/**
 * Verify by reading the snapshot of the chunk
 * @param leaderId    Primary ID
 * @param logicPoolId Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId   chunk id
 * @param length    The length of each IO
 * @param fillCh    Characters filled in each IO
 * @param loop      The number of times repeatedly initiates IO
 */
void ReadSnapshotVerify(Peer leaderPeer,
                        LogicPoolID logicPoolId,
                        CopysetID copysetId,
                        ChunkID chunkId,
                        int length,
                        char fillCh,
                        int loop);

/**
 *Delete snapshot of chunk for verification
 * @param leaderId    Primary ID
 * @param logicPoolId Logical Pool ID
 * @param copysetId   Copy Group ID
 * @param chunkId     chunk id
 * @param csn         corrected sn
 */
void DeleteSnapshotVerify(Peer leaderPeer,
                          LogicPoolID logicPoolId,
                          CopysetID copysetId,
                          ChunkID chunkId,
                          uint64_t csn);

/**
 *Abnormal I/O verification, read data does not meet expectations
 * @param leaderId    Primary ID
 * @param logicPoolId Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId   chunk id
 * @param length    The length of each IO
 * @param fillCh    Characters filled in each IO
 * @param loop      The number of times repeatedly initiates IO
 */
void ReadNotVerify(Peer leaderPeer,
                   LogicPoolID logicPoolId,
                   CopysetID copysetId,
                   ChunkID chunkId,
                   int length,
                   char fillCh,
                   int loop);

/**
 * Verify availability through read
 * @param leaderId    Primary ID
 * @param logicPoolId Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId   chunk id
 * @param length    The length of each IO
 * @param fillCh    Characters filled in each IO
 * @param loop      The number of times repeatedly initiates IO
 */
void ReadVerifyNotAvailable(Peer leaderPeer,
                            LogicPoolID logicPoolId,
                            CopysetID copysetId,
                            ChunkID chunkId,
                            int length,
                            char fillCh,
                            int loop);

/**
 * Verify availability through write
 * @param leaderId    Primary ID
 * @param logicPoolId Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId   chunk id
 * @param length    The length of each IO
 * @param fillCh    Characters filled in each IO
 * @param loop      The number of times repeatedly initiates IO
 */
void WriteVerifyNotAvailable(Peer leaderPeer,
                             LogicPoolID logicPoolId,
                             CopysetID copysetId,
                             ChunkID chunkId,
                             int length,
                             char fillCh,
                             int loop);

/**
 * Verify if the copyset status of several replicas is consistent
 * @param peerIds: Peers to be verified
 * @param logicPoolID: Logical Pool ID
 * @param copysetId: Copy group ID
 */
void CopysetStatusVerify(const std::vector<Peer> &peers,
                         LogicPoolID logicPoolID,
                         CopysetID copysetId,
                         uint64_t expectEpoch = 0);

/**
 * transfer leader and expected to succeed
 * @param cluster: Pointer to the cluster
 * @param targetLeader: The target node for the expected transfer
 * @param opt: The clioption used in the transfer request
 */
void TransferLeaderAssertSuccess(PeerCluster *cluster,
                                 const Peer &targetLeader,
                                 braft::cli::CliOptions opt);

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_INTEGRATION_COMMON_PEER_CLUSTER_H_
