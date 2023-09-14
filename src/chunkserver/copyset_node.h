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
#include <braft/repeated_timer_task.h>
#include <bthread/condition_variable.h>

#include <condition_variable>
#include <string>
#include <vector>
#include <climits>
#include <memory>
#include <deque>

#include "src/chunkserver/concurrent_apply/concurrent_apply.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/conf_epoch_file.h"
#include "src/chunkserver/config_info.h"
#include "src/chunkserver/chunkserver_metrics.h"
#include "src/chunkserver/raftlog/curve_segment_log_storage.h"
#include "src/chunkserver/raftsnapshot/define.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_writer.h"
#include "src/common/string_util.h"
#include "src/common/concurrent/task_thread_pool.h"
#include "src/chunkserver/raft_node.h"
#include "proto/heartbeat.pb.h"
#include "proto/chunk.pb.h"
#include "proto/common.pb.h"
#include "proto/scan.pb.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::curve::mds::heartbeat::ConfigChangeType;
using ::curve::common::Peer;
using ::curve::common::TaskThreadPool;

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
    // Current configuration change information in the copyset node
    std::shared_ptr<ConfigurationChange> curCfgChange;
    // The configuration change information corresponding to this configuration change
    ConfigurationChange expectedCfgChange;
};

class CopysetNode;

class SyncChunkThread : public curve::common::Uncopyable {
 public:
    friend class CopysetNode;
    SyncChunkThread() = default;
    ~SyncChunkThread();
    void Run();
    void Init(CopysetNode* node);
    void Stop();
 private:
    bool running_;
    std::mutex mtx_;
    std::shared_ptr<std::condition_variable> cond_;
    std::thread syncThread_;
    CopysetNode* node_;
};

/**
 * A Copyset Node is a replica of a replication group
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
     * Initialize copyset node configuration
     * @param options
     * @return 0, successful, -1 failed
     */
    virtual int Init(const CopysetNodeOptions &options);

    /**
     * Raft Node init to make Raft Node run
     * @return
     */
    virtual int Run();

    /**
     * Close copyset node
     */
    virtual void Fini();

    /**
     * Returns the logical pool ID of the replication group
     * @return
     */
    LogicPoolID GetLogicPoolId() const;

    /**
     * Returns the replication group ID of the replication group
     * @return
     */
    CopysetID GetCopysetId() const;

    virtual void SetScan(bool scan);

    virtual bool GetScan() const;

    virtual void SetLastScan(uint64_t time);

    virtual uint64_t GetLastScan() const;

    virtual std::vector<ScanMap>& GetFailedScanMap();

    /**
     * Return to the replication group data directory
     * @return
     */
    std::string GetCopysetDir() const;

    /**
     * Returns whether the current replica is in the leader's tenure
     * @return
     */
    virtual bool IsLeaderTerm() const;

    /**
     * check if current node is in lease leader
     * @return
     */
    virtual bool IsLeaseLeader(const braft::LeaderLeaseStatus &lease_status) const;  // NOLINT

    /**
     * check if current node is expired
     * @return
     */
    virtual bool IsLeaseExpired(const braft::LeaderLeaseStatus &lease_status) const;  // NOLINT

    /**
     * Return to current tenure
     * @return Current tenure
     */
    virtual uint64_t LeaderTerm() const;

    /**
     * Return leader id
     * @return
     */
    virtual PeerId GetLeaderId() const;

    /**
     * @brief Switch the leader of the replication group
     * @param[in] peerId The member ID of the target leader
     * @return  Reference to Heartbeat Task
     */
    butil::Status TransferLeader(const Peer& peer);

    /**
     * @brief Add new members to the replication group
     * @param[in] peerId The ID of the new member
     * @return  Reference to Heartbeat Task
     */
    butil::Status AddPeer(const Peer& peer);

    /**
     * @brief Copy Group Delete Members
     * @param[in] peerId The ID of the member to be deleted
     * @return  Reference to Heartbeat Task
     */
    butil::Status RemovePeer(const Peer& peer);

    /**
     * @brief Change replication group members
     * @param[in] newPeers New replication group member
     * @return  Reference to Heartbeat Task
     */
    butil::Status ChangePeer(const std::vector<Peer>& newPeers);

    /**
     * Returns the configuration version of the copyset
     * @return
     */
    virtual uint64_t GetConfEpoch() const;

    /**
     * Update the applied index, only those larger than it will be updated
     * @param index
     */
    virtual void UpdateAppliedIndex(uint64_t index);

    /**
     * Returns the current latest applied index
     * @return
     */
    virtual uint64_t GetAppliedIndex() const;

    /**
     * @brief: Query the status of configuration changes
     * @param type[out]: Configuration change type
     * @param oldConf[out]: Old configuration
     * @param alterPeer[out]: Changed Peer
     * @return 0 query successful, -1 query exception failed
     */
    virtual int GetConfChange(ConfigChangeType *type,
                              Configuration *oldConf,
                              Peer *alterPeer);

    /**
     * @brief: Obtain the status value of the copyset node for comparing data consistency across multiple replicas
     * @param hash[out]: copyset node status value
     * @return 0 succeeded, -1 failed
     */
    virtual int GetHash(std::string *hash);

    /**
     * @brief:  Get the status of the copyset node, actually calling the get_status interface of the Raft node
     * @param status[out]: copyset node status
     */
    virtual void GetStatus(NodeStatus *status);

    /**
     * @brief: get raft node leader lease status
     * @param status[out]: raft node leader lease status
     */
    virtual void GetLeaderLeaseStatus(braft::LeaderLeaseStatus *status);

    /**
     * Obtain the status on the leader of this copyset
     * @param leaderStaus[out]: leader copyset node status
     * @return returns true for successful acquisition, false for failed acquisition
     */
    virtual bool GetLeaderStatus(NodeStatus *leaderStaus);

    /**
     * Return data store pointer
     * @return
     */
    virtual std::shared_ptr<CSDataStore> GetDataStore() const;

    /**
     * @brief: Get braft log storage
     * @return: The pointer to CurveSegmentLogStorage
     */
    virtual CurveSegmentLogStorage* GetLogStorage() const;

    /**
     * Returning ConcurrentApplyModule
     */
    virtual ConcurrentApplyModule* GetConcurrentApplyModule() const;

    /**
     * Propose an op request to the copyset node
     * @param task
     */
    virtual void Propose(const braft::Task &task);

    /**
     * Get replication group members
     * @param peers: List of returned members (output parameters)
     * @return
     */
    virtual void ListPeers(std::vector<Peer>* peers);

    /**
     * @brief initialize raft node options corresponding to the copyset node
     * @param options
     * @return
     */
    void InitRaftNodeOptions(const CopysetNodeOptions &options);

    /**
     * The following interfaces are all interfaces that inherit the implementation of StateMachine
     */
 public:
    /**
     * Callback function when applying op log
     * @param iter: Allows batch access to already committed log entries.
     */
    void on_apply(::braft::Iterator &iter) override;

    /**
     * Call this callback when replication is closed
     */
    void on_shutdown() override;

    /**
     * Interfaces related to raft snapshot, which only store raft snapshot meta
     * and a list of snapshot files. Actual data is not copied here because
     * in the context of block storage, all operations are idempotent, so there is no need to actually copy the data.
     */
    void on_snapshot_save(::braft::SnapshotWriter *writer,
                          ::braft::Closure *done) override;

    /**
     * There are two scenarios for loading logs:
     *  1. Follower nodes catch up with the leader by installing a snapshot. In this case,
     *    there are chunk data and snapshot data under the snapshot directory.
     *  2. When a node restarts, it performs a snapshot load and then replays the logs. In
     *    this case, there is no data under the snapshot directory, so nothing needs to be done.
     *  TODO(wudemiao): When installing a snapshot, there is a possibility of doubling
     *  the space usage. Consider the following scenario: a follower lags behind and then
     *  recovers data by installing a snapshot from the leader. It will first download all
     *  the data from the leader and then call snapshot load to load the snapshot. During
     *  this period, the space usage doubles. Later, we need to control the number of disks
     *  participating in the installation of snapshots.
     */
    int on_snapshot_load(::braft::SnapshotReader *reader) override;

    /**
     * The new leader will call this interface after applying noop, indicating that this leader can provide read/write services.
     * @param term: Current leader term
     */
    void on_leader_start(int64_t term) override;

    /**
     * Called when the leader step is down
     * @param status: The status of the replication group
     */
    void on_leader_stop(const butil::Status &status) override;

    /**
     * Called when an error occurs in the replication group
     * @param e: Specific error
     */
    void on_error(const ::braft::Error &e) override;

    /**
     * This function will be called when configuring the change log entry application, and currently this interface will be utilized
     * Update configuration epoch value
     * @param conf: The latest configuration of the current replication group
     * @param index log index
     */
    void on_configuration_committed(const Configuration& conf, int64_t index) override;   //NOLINT

    /**
     * Called when the follower stops following the main
     * @param ctx: Can obtain the reason for stop following
     */
    void on_stop_following(const ::braft::LeaderChangeContext &ctx) override;

    /**
     * Called after the Follower or Candidate finds a new leader
     * @param ctx: Change the leader up and down to obtain the reasons for the new leader and start following
     */
    void on_start_following(const ::braft::LeaderChangeContext &ctx) override;

    /**
     * Used for testing injection mock dependencies
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
    // sync trigger seconds
    static uint32_t syncTriggerSeconds_;
    // shared to sync pool
    static std::shared_ptr<TaskThreadPool<>> copysetSyncPool_;
    /**
     * Parsing copyset configuration version information from a file
     * @param filePath: File path
     * @return 0: success, -1 fail
     */
    int LoadConfEpoch(const std::string &filePath);

    /**
     * Save the copyset configuration version information to a file
     * @param filePath: File path
     * @return 0 success, -1 fail
     */
    int SaveConfEpoch(const std::string &filePath);

 public:
    void save_snapshot_background(::braft::SnapshotWriter *writer,
                                  ::braft::Closure *done);

    void ShipToSync(ChunkID chunkId) {
        if (enableOdsyncWhenOpenChunkFile_) {
            return;
        }

        curve::common::LockGuard lg(chunkIdsLock_);
        chunkIdsToSync_.push_back(chunkId);
    }

    void HandleSyncTimerOut();

    void SyncAllChunks();

    void ForceSyncAllChunks();

    void WaitSnapshotDone();

 private:
    inline std::string GroupId() {
        return ToGroupId(logicPoolId_, copysetId_);
    }

    inline std::string GroupIdString() {
        return ToGroupIdString(logicPoolId_, copysetId_);
    }

 private:
    // Logical Pool ID
    LogicPoolID logicPoolId_;
    // Copy Group ID
    CopysetID copysetId_;
    // Configuration of replication groups
    Configuration       conf_;
    // Configuration operation lock for replication group
    mutable std::mutex  confLock_;
    // Copy the configuration version of the group
    std::atomic<uint64_t> epoch_;
    // Peer ID of the replication group replica
    PeerId peerId_;
    // Configuration parameters for the braft Node
    NodeOptions nodeOptions_;
    // The braft Node corresponding to CopysetNode
    std::shared_ptr<RaftNode> raftNode_;
    // Absolute directory for chunk files
    std::string chunkDataApath_;
    // Relative directory for chunk files
    std::string chunkDataRpath_;
    // copyset absolute path
    std::string copysetDirPath_;
    // File system adapter
    std::shared_ptr<LocalFileSystem> fs_;
    // Chunk Persistence Operation Interface
    std::shared_ptr<CSDataStore> dataStore_;
    // The log storage for braft
    CurveSegmentLogStorage* logStorage_;
    // Concurrent module
    ConcurrentApplyModule *concurrentapply_ = nullptr;
    // Configure version persistence tool interface
    std::unique_ptr<ConfEpochFile> epochFile_;
    // Apply index of replication group
    std::atomic<uint64_t> appliedIndex_;
    // Copy the current tenure of the group. If<=0, it indicates that it is not a leader
    std::atomic<int64_t> leaderTerm_;
    // Copy Group Data Recycle Bin Directory
    std::string recyclerUri_;
    // Copy the metric information of the group
    CopysetMetricPtr metric_;
    // Configuration changes in progress
    std::shared_ptr<ConfigurationChange> configChange_;
    // The target of the transfer leader is valid when the status is TRANSFERRING
    Peer transferee_;
    int64_t lastSnapshotIndex_;
    // scan status
    bool scaning_;
    // last scan time
    uint64_t lastScanSec_;
    // failed check scanmap
    std::vector<ScanMap> failedScanMaps_;

    // enable O_DSYNC when open file
    bool enableOdsyncWhenOpenChunkFile_;
    // sync chunk thread
    SyncChunkThread syncThread_;
    // chunkIds need to sync
    std::deque<ChunkID> chunkIdsToSync_;
    // lock for chunkIdsToSync_
    mutable curve::common::Mutex chunkIdsLock_;
    // is syncing
    std::atomic<bool> isSyncing_;
    // do snapshot check syncing interval
    uint32_t checkSyncingIntervalMs_;
    // async snapshot future object
    std::future<void> snapshotFuture_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_COPYSET_NODE_H_
