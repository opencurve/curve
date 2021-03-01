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
 * Created Date: 18-8-23
 * Author: wudemiao
 */

#ifndef SRC_CHUNKSERVER_COPYSET_NODE_H_
#define SRC_CHUNKSERVER_COPYSET_NODE_H_

#include <butil/memory/ref_counted.h>

#include <string>
#include <vector>
#include <climits>
#include <memory>

#include "src/chunkserver/concurrent_apply/concurrent_apply.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/conf_epoch_file.h"
#include "src/chunkserver/config_info.h"
#include "src/chunkserver/chunkserver_metrics.h"
#include "src/chunkserver/raftsnapshot/define.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_writer.h"
#include "src/common/string_util.h"
#include "src/chunkserver/raft_node.h"
#include "proto/heartbeat.pb.h"
#include "proto/chunk.pb.h"
#include "proto/common.pb.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::curve::mds::heartbeat::ConfigChangeType;
using ::curve::common::Peer;

class CopysetNodeManager;

extern const char *kCurveConfEpochFilename;

struct ConfigurationChange {
    ConfigChangeType type;
    Peer alterPeer;

    ConfigurationChange() : type(ConfigChangeType::NONE) {}
    ConfigurationChange(const ConfigChangeType& type2, const Peer& alterPeer2) :
                            type(type2), alterPeer(alterPeer2) {}
    bool IsEmpty() {
        return type == ConfigChangeType::NONE && !alterPeer.has_address();
    }
    void Reset() {
        type = ConfigChangeType::NONE;
        alterPeer.clear_address();
    }
    bool operator==(const ConfigurationChange& rhs) {
        return type == rhs.type &&
                alterPeer.address() == rhs.alterPeer.address();
    }
    ConfigurationChange& operator=(const ConfigurationChange& rhs) {
        type = rhs.type;
        alterPeer = rhs.alterPeer;
        return *this;
    }
};

class ConfigurationChangeDone : public braft::Closure {
 public:
    void Run() {
        if (!expectedCfgChange.IsEmpty() &&
                            *curCfgChange == expectedCfgChange) {
            curCfgChange->Reset();
        }
        delete this;
    }
    explicit ConfigurationChangeDone(
                std::shared_ptr<ConfigurationChange> cfgChange)
                    : curCfgChange(cfgChange) {}
    // Information about current configuration changes in the copyset node
    std::shared_ptr<ConfigurationChange> curCfgChange;
    // Configuration change information
    ConfigurationChange expectedCfgChange;
};

/**
 * A Copyset Node is a copy of a copyset
 */
class CopysetNode : public braft::StateMachine,
                    public std::enable_shared_from_this<CopysetNode> {
 public:
    // for ut mock
    CopysetNode() = default;

    CopysetNode(const LogicPoolID &logicPoolId,
                const CopysetID &copysetId,
                const Configuration &initConf);

    virtual ~CopysetNode();

    /**
     * Init copyset node configurations
     * @param options
     * @return Return 0 for success, -1 for failure
     */
    virtual int Init(const CopysetNodeOptions &options);

    /**
     * Raft Node init，make Raft Node run
     * @return
     */
    virtual int Run();

    /**
     * Shut down copyset node
     */
    virtual void Fini();

    /**
     * Logic pool ID
     * @return
     */
    LogicPoolID GetLogicPoolId() const;

    /**
     * Copyset ID
     * @return
     */
    CopysetID GetCopysetId() const;

    /**
     * Copyset Directories
     * @return
     */
    std::string GetCopysetDir() const;

    /**
     * Whether it is in leader term
     * @return
     */
    virtual bool IsLeaderTerm() const;

    /**
     * Return current term
     * @return Current term
     */
    virtual uint64_t LeaderTerm() const;

    /**
     * Leader id
     * @return
     */
    virtual PeerId GetLeaderId() const;

    /**
     * @brief Transfer copyset's Leader
     * @param[in] peerId Target Leader's peer ID
     * @return Reference for Heartbeat Task
     */
    butil::Status TransferLeader(const Peer& peer);

    /**
     * @brief Add a peer
     * @param[in] peerId New peer id
     * @return Reference for Heartbeat Task
     */
    butil::Status AddPeer(const Peer& peer);

    /**
     * @brief Remove a peer
     * @param[in] peerId Peer id to be removed
     * @return Reference for Heartbeat Task
     */
    butil::Status RemovePeer(const Peer& peer);

    /**
     * @brief Change a peer
     * @param[in] newPeers New peers
     * @return Reference for Heartbeat Task
     */
    butil::Status ChangePeer(const std::vector<Peer>& newPeers);

    /**
     * Return the configuration epoch of copyset
     * @return
     */
    virtual uint64_t GetConfEpoch() const;

    /**
     * Uodate applied index，only for those that are bigger than the original
     * @param index
     */
    virtual void UpdateAppliedIndex(uint64_t index);

    /**
     * Return the latest applied index
     * @return
     */
    virtual uint64_t GetAppliedIndex() const;

    /**
     * @brief: Get the status of configuration changes
     * @param type[out]: Configuration changes type
     * @param oldConf[out]: Old configuration
     * @param alterPeer[out]: Altered peer
     * @return Return 0 for success, -1 for failure
     */
    virtual int GetConfChange(ConfigChangeType *type,
                              Configuration *oldConf,
                              Peer *alterPeer);

    /**
     * @brief: Get the state value of the copyset node for comparing data
     * consistency across multiple copies
     * @param hash[out]: copyset node status value
     * @return Return 0 for success, -1 for failure
     */
    virtual int GetHash(std::string *hash);

    /**
     * @brief: Get copyset node's status，actually call the get_status interface
     * of the raft node
     * @param status[out]: copyset node status
     */
    virtual void GetStatus(NodeStatus *status);

    /**
     * Get the status on the leader of this copyset
     * @param leaderStaus[out]: leader copyset node status
     * @return Return true for success, false for failure
     */
    virtual bool GetLeaderStatus(NodeStatus *leaderStaus);

    /**
     * Return data store pointer
     * @return
     */
    virtual std::shared_ptr<CSDataStore> GetDataStore() const;

    /**
     * Return ConcurrentApplyModule
     */
    virtual ConcurrentApplyModule* GetConcurrentApplyModule() const;

    /**
     * Propose a op request to copyset node
     * @param task
     */
    virtual void Propose(const braft::Task &task);

    /**
     * Get peers
     * @param peers:List of returned peers (dump parameters)
     * @return
     */
    void ListPeers(std::vector<Peer>* peers);

    /**
     * The following interfaces are all interfaces that inherit from the
     * StateMachine implementation
     */
 public:
    /**
     * Callback function when op log apply
     * @param iter:Can batch access log entries that have been committed
     */
    void on_apply(::braft::Iterator &iter) override;

    /**
     * Call this callback when the copy is closed
     */
    void on_shutdown() override;

    /**
     * raft snapshot-related interface, which only holds raft snapshot meta
     * and the list of snapshot files, does not copy the actual data here,
     * as all operations in a block storage scenario are idempotent, so no
     * real data is copied
     */
    void on_snapshot_save(::braft::SnapshotWriter *writer,
                          ::braft::Closure *done) override;

    /**
     *  The load log has two scenarios:
     *  1. Follower node Install snapshot catches up with the leader, at this
     *  time, there is chunk data and snapshot data under the snapshot directory
     *  2. The node restarts, it performs a snapshot load and then plays back
     *  the logs, at this time there is no data in the snapshot directory and
     *  nothing needs to be done
     *  TODO(wudemiao): There is a possibility of a space double when
     *   installing  snapshot.
     *  Consider the following scenario, where follower falls behind and then
     *  restores data by installing snapshot from leader. It will first download
     *  all the data from the leader and then call snapshot load to load the
     *  snapshot, which doubles the space in the meantime. Later on, we need to
     *   control the number of install snapshot on a single disk.
     */
    int on_snapshot_load(::braft::SnapshotReader *reader) override;

    /**
     * This interface is called by the new leader after apply noop,
     * indicating that the leader is ready to provide read/write services.
     * @param term:Current leader term
     */
    void on_leader_start(int64_t term) override;

    /**
     * Call when leader step down
     * @param status:Copyset status
     */
    void on_leader_stop(const butil::Status &status) override;

    /**
     * Call when an error occurs in the copyset
     * @param e:Specific error
     */
    void on_error(const ::braft::Error &e) override;

    /**
     * This function is called when the configuration changelog entry
     * applies, and this interface is currently used to update the
     * configuration epoch values
     * @param conf:The latest configuration of the current copyset
     * @param index log index
     */
    void on_configuration_committed(const Configuration& conf, int64_t index) override;   //NOLINT

    /**
     * Call when follower stops following the leader
     * @param ctx:Get the reason for stop following
     */
    void on_stop_following(const ::braft::LeaderChangeContext &ctx) override;

    /**
     * Called by Follower or Candidate when a new leader is found
     * @param ctx:When the leader changes, you can get the new leader and the
     * reason for starting following
     */
    void on_start_following(const ::braft::LeaderChangeContext &ctx) override;

    /**
     * Test injection of mock dependencies
     */
 public:
    void SetCSDateStore(std::shared_ptr<CSDataStore> datastore);

    void SetLocalFileSystem(std::shared_ptr<LocalFileSystem> fs);

    void SetConfEpochFile(std::unique_ptr<ConfEpochFile> epochFile);

    void SetCopysetNode(std::shared_ptr<RaftNode> node);

    void SetSnapshotFileSystem(scoped_refptr<FileSystemAdaptor>* fs);

    /**
     * better for test
     */
 public:
    /**
     * Load copyset configuration epoch information from files
     * @param filePath:File path
     * @return 0: successs, -1 failed
     */
    int LoadConfEpoch(const std::string &filePath);

    /**
     * Save copyset configuration epoch information to a file
     * @param filePath:File path
     * @return Return 0 for success, -1 for failure
     */
    int SaveConfEpoch(const std::string &filePath);

 private:
    inline std::string GroupId() {
        return ToGroupId(logicPoolId_, copysetId_);
    }

    inline std::string GroupIdString() {
        return ToGroupIdString(logicPoolId_, copysetId_);
    }

 private:
    // logicPool id
    LogicPoolID logicPoolId_;
    // copyset id
    CopysetID copysetId_;
    // copyset configuration
    Configuration       conf_;
    // copyset configuration operation locks
    mutable std::mutex  confLock_;
    // copyset configuration epoch
    std::atomic<uint64_t> epoch_;
    // peer id of one copy in the copyset
    PeerId peerId_;
    // braft Node configuration options
    NodeOptions nodeOptions_;
    // CopysetNode's corresponding braft Node
    std::shared_ptr<RaftNode> raftNode_;
    // Absolute directory for chunk file
    std::string chunkDataApath_;
    // Relative directory for chunk file
    std::string chunkDataRpath_;
    // Absolute path for copyset
    std::string copysetDirPath_;
    // File System Adapters
    std::shared_ptr<LocalFileSystem> fs_;
    // Chunk persistence interface
    std::shared_ptr<CSDataStore> dataStore_;
    // Concurrent modules
    ConcurrentApplyModule *concurrentapply_;
    // Configuration epoch persistence tool interface
    std::unique_ptr<ConfEpochFile> epochFile_;
    // Copyset's apply index
    std::atomic<uint64_t> appliedIndex_;
    // Copyset's current term，If <= 0 means not a leader
    std::atomic<int64_t> leaderTerm_;
    // Data recycle uri
    std::string recyclerUri_;
    // Metric information of copyset
    CopysetMetricPtr metric_;
    // Ongoing configuration changes
    std::shared_ptr<ConfigurationChange> configChange_;
    // Target of transfer leader, valid when status is TRANSFERRING
    Peer transferee_;
    int64_t lastSnapshotIndex_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_COPYSET_NODE_H_
