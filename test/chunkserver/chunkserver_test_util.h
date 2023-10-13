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

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/datastore/file_pool.h"
#include "src/fs/local_filesystem.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;

std::string Exec(const char* cmd);

/**
 * The current FilePool needs to be formatted in advance before it can be used.
 * This function is used to format the FilePool in advance
 * @param fsptr: This article's file system pointer
 * @param chunkfileSize: Chunk file size
 * @param metaPageSize: The metapage size of the chunk file
 * @param poolpath: The path to the file pool, for example ./chunkfilepool/
 * @param metaPath: meta file path, for example
 * ./chunkfilepool/chunkfilepool.meta
 * @return successfully initializes and returns the FilePool pointer. Otherwise,
 * it returns null
 */
std::shared_ptr<FilePool> InitFilePool(
    std::shared_ptr<LocalFileSystem> fsptr,  // NOLINT
    int chunkfileCount, int chunkfileSize, int metaPageSize,
    std::string poolpath, std::string metaPath);

int StartChunkserver(const char* ip, int port, const char* copysetdir,
                     const char* confs, const int snapshotInterval,
                     const int electionTimeoutMs);

butil::Status WaitLeader(const LogicPoolID& logicPoolId,
                         const CopysetID& copysetId, const Configuration& conf,
                         PeerId* leaderId, int electionTimeoutMs);

/**
 * PeerNode status
 * 1. exit: Not started or closed
 * 2. running: Running
 * 3. stop: hang
 */
enum class PeerNodeState {
    EXIT = 0,     // Exit
    RUNNING = 1,  // Running
    STOP = 2,     // Hang Stay
};

/**
 * A ChunkServer process that contains a copy of a Copyset
 */
struct PeerNode {
    PeerNode() : pid(0), options(), state(PeerNodeState::EXIT) {}
    // Process ID corresponding to Peer
    pid_t pid;
    // Peer's address
    PeerId peerId;
    // Cluster configuration for copyset
    Configuration conf;
    // Basic configuration of copyset
    CopysetNodeOptions options;
    // Status of PeerNode
    PeerNodeState state;
};

/**
 * Package simulation cluster testing related interfaces
 */
class TestCluster {
 public:
    TestCluster(const std::string& clusterName, const LogicPoolID logicPoolID,
                const CopysetID copysetID, const std::vector<PeerId>& peers);
    virtual ~TestCluster() { StopAllPeers(); }

 public:
    /**
     * Start a Peer
     * @param peerId
     * @param empty Is the initialization configuration empty
     * @param: get_chunk_from_pool Does obtain a chunk from FilePool
     * @param: createFilePool: create a FilePool? It is not necessary to restart
     * it
     * @return 0: Success, -1 failed
     */
    int StartPeer(const PeerId& peerId, const bool empty = false,
                  bool getChunkFrom_pool = false, bool createFilePool = true);
    /**
     * Close a peer and use SIGINT
     * @param peerId
     * @return 0: Success, -1 failed
     */
    int ShutdownPeer(const PeerId& peerId);

    /**
     * Hang lives in a peer and uses SIGSTOP
     * @param peerId
     * @return 0: Success, -1 failed
     */
    int StopPeer(const PeerId& peerId);
    /**
     * Restore the peer where Hang lives and use SIGCONT
     * @param peerId
     * @return 0: Success, -1 failed
     */
    int ContPeer(const PeerId& peerId);
    /**
     * Try again and again until a new leader is generated
     * @param leaderId takes a parameter and returns the leader id
     * @return 0: Success, -1 failed
     */
    int WaitLeader(PeerId* leaderId);

    /**
     * Stop all peers
     * @return 0: Success, -1 failed
     */
    int StopAllPeers();

 public:
    /*Returns the current configuration of the cluster*/
    const Configuration CopysetConf() const;

    /*Modify the interface related to PeerNode configuration, unit: s*/
    int SetsnapshotIntervalS(int snapshotIntervalS);
    int SetElectionTimeoutMs(int electionTimeoutMs);
    int SetCatchupMargin(int catchupMargin);

    static int StartPeerNode(CopysetNodeOptions options,
                             const Configuration conf,
                             bool from_chunkfile_pool = false,
                             bool createFilePool = true);

 public:
    /**
     * Returns the copyset path for executing peer with protocol, ex:
     * local://./127.0.0.1:9101:0
     */
    static const std::string CopysetDirWithProtocol(const PeerId& peerId);
    /**
     * Returns the copyset path for executing peer without protocol, ex:
     * ./127.0.0.1:9101:0
     */
    static const std::string CopysetDirWithoutProtocol(const PeerId& peerId);
    /**
     * remove peer's copyset dir's cmd
     */
    static const std::string RemoveCopysetDirCmd(const PeerId& peerid);

 private:
    // Cluster Name
    std::string clusterName_;
    // The peer set of the cluster
    std::set<PeerId> peers_;
    // Mapping Map of Peer Set
    std::unordered_map<std::string, std::unique_ptr<PeerNode>> peersMap_;

    // Snapshot interval
    int snapshotIntervalS_;
    // Election timeout
    int electionTimeoutMs_;
    // Catchup margin configuration
    int catchupMargin_;
    // Cluster member configuration
    Configuration conf_;

    // Logical Pool ID
    static LogicPoolID logicPoolID_;
    // Copy Group ID
    static CopysetID copysetID_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_CHUNKSERVER_TEST_UTIL_H_
