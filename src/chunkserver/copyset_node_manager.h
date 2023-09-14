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

#ifndef SRC_CHUNKSERVER_COPYSET_NODE_MANAGER_H_
#define SRC_CHUNKSERVER_COPYSET_NODE_MANAGER_H_

#include <mutex>    //NOLINT
#include <vector>
#include <memory>
#include <unordered_map>

#include "src/chunkserver/copyset_node.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/uncopyable.h"
#include "src/common/concurrent/task_thread_pool.h"

namespace curve {
namespace chunkserver {

using curve::common::BthreadRWLock;
using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using curve::common::TaskThreadPool;

class ChunkOpRequest;

/**
 * Manager of Copyset Node
 */
class CopysetNodeManager : public curve::common::Uncopyable {
 public:
    using CopysetNodePtr = std::shared_ptr<CopysetNode>;

    // Single example, only correct in c++11 or higher versions
    static CopysetNodeManager &GetInstance() {
        static CopysetNodeManager instance;
        return instance;
    }
    virtual ~CopysetNodeManager() = default;
    int Init(const CopysetNodeOptions &copysetNodeOptions);
    int Run();
    int Fini();

    /**
     * @brief Load all copysets in the directory
     *
     * @return 0 indicates successful loading, non 0 indicates failed loading
     */
    int ReloadCopysets();

    /**
     * To create a copyset node, there are two situations where you need to create a copyset node
     * TODO(wudemiao): Delete after later replacement
     *  1. Cluster initialization, creating copyset
     *  2. add peer during recovery
     */
    bool CreateCopysetNode(const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId,
                           const Configuration &conf);

    /**
     * Both are creating copysets, currently both exist simultaneously, and only one will be retained in the future
     */
    bool CreateCopysetNode(const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId,
                           const std::vector<Peer> peers);

    /**
     * Delete the copyset node memory instance (stop copyset, destroy the copyset memory instance, and remove it from the copyset
     * Clearing the copyset table entry in the manager's copyset table does not affect the persistence data of the copyset on the disk
     * @param logicPoolId: Logical Pool ID
     * @param copysetId: Copy group ID
     * @return true succeeded, false failed
     */
    bool DeleteCopysetNode(const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId);

    /**
     * Completely delete the copyset node's memory data (stop copyset, destroy the copyset memory instance, and remove it from the
     * Clear the copyset table entries in the copyset manager's copyset table and persist the copyset data from the disk
     * Completely delete on)
     * @param logicPoolId: Logical Pool ID
     * @param copysetId: Copy group ID
     * @return true succeeded, false failed
     */
    bool PurgeCopysetNodeData(const LogicPoolID &logicPoolId,
                              const CopysetID &copysetId);

    /**
     * @brief Delete broken copyset
     * @param[in] poolId logical pool id
     * @param[in] copysetId copyset id
     * @return true if delete success, else return false
     */ 
    bool DeleteBrokenCopyset(const LogicPoolID& poolId,
                             const CopysetID& copysetId);

    /**
     * Determine whether the specified copyset exists
     * @param logicPoolId: Logical Pool ID
     * @param copysetId: Copy group ID
     * @return true exists, false does not exist
     */
    bool IsExist(const LogicPoolID &logicPoolId, const CopysetID &copysetId);

    /**
     * Get the specified copyset
     * @param logicPoolId: Logical Pool ID
     * @param copysetId: Copy group ID
     * @return nullptr means that no query was found
     */
    virtual CopysetNodePtr GetCopysetNode(const LogicPoolID &logicPoolId,
                                          const CopysetID &copysetId) const;

    /**
     * Query all copysets
     * @param nodes: Issue parameters and return all copysets
     */
    void GetAllCopysetNodes(std::vector<CopysetNodePtr> *nodes) const;

    /**
     * Add RPC service
     * TODO(wudemiao): Currently only used for testing, and will be removed after later refinement
     * @param server: rpc Server
     * @param listenAddress: The address to listen to
     * @return 0 succeeded, -1 failed
     */
    int AddService(brpc::Server *server,
                   const butil::EndPoint &listenAddress);

    virtual const CopysetNodeOptions &GetCopysetNodeOptions() const {
        return copysetNodeOptions_;
    }

    /**
     * @brief: Only for test
     */
    void SetCopysetNodeOptions(
        const CopysetNodeOptions& copysetNodeOptions) {
        copysetNodeOptions_ = copysetNodeOptions;
    }

    /**
     * Load copyset, including creating a new copyset or restarting a copyset
     * @param logicPoolId: Logical Pool ID
     * @param copysetId: copyset id
     * @param needCheckLoadFinished: Do you need to determine if the copyset loading is complete
     */
    void LoadCopyset(const LogicPoolID &logicPoolId,
                     const CopysetID &copysetId,
                     bool needCheckLoadFinished);
    /**
     * Detect the specified copyset state until the copyset load is completed or an exception occurs
     * @param node: The specified copyset node
     * @return true indicates successful loading, while false indicates an exception occurred during the detection process
     */
    bool CheckCopysetUntilLoadFinished(std::shared_ptr<CopysetNode> node);

    /**
     * Obtain the status of copysetNodeManager loading copyset
     * @return false-copyset not loaded complete, true-copyset loaded complete
     */
    virtual bool LoadFinished();

 protected:
    CopysetNodeManager()
        : copysetLoader_(nullptr)
        , running_(false)
        , loadFinished_(false) {}

 private:
    /**
     * If the specified copyset does not exist, insert the copyset into the map (thread safe)
     * @param logicPoolId: Logical Pool ID
     * @param copysetId: Copy group ID
     * @param node: The copysetnode to be inserted
     * @return If the copyset does not exist, insert it into the map and return true;
     *         If copyset exists, return false
     */
    bool InsertCopysetNodeIfNotExist(const LogicPoolID &logicPoolId,
                                     const CopysetID &copysetId,
                                     std::shared_ptr<CopysetNode> node);

    /**
     * Create a new copyset or load an existing copyset (non thread safe)
     * @param logicPoolId: Logical Pool ID
     * @param copysetId: Copy group ID
     * @param conf: The configuration members of this copyset
     * @return Successfully created or loaded, returns copysetnode, otherwise returns nullptr
     */
    std::shared_ptr<CopysetNode> CreateCopysetNodeUnlocked(
        const LogicPoolID &logicPoolId,
        const CopysetID &copysetId,
        const Configuration &conf);

 private:
    using CopysetNodeMap = std::unordered_map<GroupId,
                                              std::shared_ptr<CopysetNode>>;
    // Protect the read write lock of the replication group map
    mutable BthreadRWLock rwLock_;
    // Copy Group Map
    CopysetNodeMap copysetNodeMap_;
    // Copy Group Configuration Options
    CopysetNodeOptions copysetNodeOptions_;
    // Control the number of concurrent starts of copyset
    std::shared_ptr<TaskThreadPool<>> copysetLoader_;
    // Indicates whether the copyset node manager is currently running
    Atomic<bool> running_;
    // Indicates whether the copyset node manager has currently completed loading
    Atomic<bool> loadFinished_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_COPYSET_NODE_MANAGER_H_
