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

using curve::common::RWLock;
using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using curve::common::TaskThreadPool;

class ChunkOpRequest;

/**
 * Copyset Node manager
 */
class CopysetNodeManager : public curve::common::Uncopyable {
 public:
    using CopysetNodePtr = std::shared_ptr<CopysetNode>;

    // Singleton, correct only under C++11 or higher
    static CopysetNodeManager &GetInstance() {
        static CopysetNodeManager instance;
        return instance;
    }

    int Init(const CopysetNodeOptions &copysetNodeOptions);
    int Run();
    int Fini();

    /**
     * @brief Load all copysets in the directory
     *
     * @return 0 for successful loading, non-zero for failed loading
     */
    int ReloadCopysets();

    /**
     * The copyset node needs to be created in the following two cases
     * TODO(wudemiao): Delete after later replacement
     *  1.Initialize the Cluster, and create the copyset
     *  2.Add peer at the time of recovery
     */
    bool CreateCopysetNode(const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId,
                           const Configuration &conf);

    /**
     * Both functions create copyset and currently exist simultaneously, keep just one later
     */
    bool CreateCopysetNode(const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId,
                           const std::vector<Peer> peers);

    /**
     * Delete the copyset node memory instance (Stop copyset, destroy the copyset memory
     * instance and clear the copyset table entries from the copyset table in the
     * copysetmanager. It does not affect the copyset persistent data on the disk)
     * @param logicPoolId:logicPool id
     * @param copysetId:copyset id
     * @return true for success, false for failure
     */
    bool DeleteCopysetNode(const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId);

    /**
     * Delete the copyset node memory data (Stop copyset, destroy the copyset memory
     * instance and clear the copyset table entries from the copyset table in the
     * copysetmanager. It does not affect the copyset persistent data on the disk)
     * @param logicPoolId:logicPool id
     * @param copysetId:copyset id
     * @return true for success, false for failure
     */
    bool PurgeCopysetNodeData(const LogicPoolID &logicPoolId,
                              const CopysetID &copysetId);

    /**
     * Check if the specified copyset exists
     * @param logicPoolId:logicPool id
     * @param copysetId:copyset id
     * @return true means it exists, false does not
     */
    bool IsExist(const LogicPoolID &logicPoolId, const CopysetID &copysetId);

    /**
     * Get the specified copyset
     * @param logicPoolId:logicPool id
     * @param copysetId:copyset id
     * @return return nullptr if fail to get
     */
    CopysetNodePtr GetCopysetNode(const LogicPoolID &logicPoolId,
                                  const CopysetID &copysetId) const;

    /**
     * Get all the copysets
     * @param nodes:return all copyset
     */
    void GetAllCopysetNodes(std::vector<CopysetNodePtr> *nodes) const;

    /**
     * Add RPC service
     * TODO(wudemiao): Currently only used for testing, will be removed later
     * @param server:rpc Server
     * @param listenAddress:the address of listening
     * @return 0 for success, -1 for failure
     */
    int AddService(brpc::Server *server,
                   const butil::EndPoint &listenAddress);

    const CopysetNodeOptions &GetCopysetNodeOptions() const {
        return copysetNodeOptions_;
    }

    /**
     * Load a copyset, including creating a new copyset or restarting a copyset
     * @param logicPoolId: logicPool id
     * @param copysetId: copyset id
     * @param needCheckLoadFinished: whether need to check if copyset loading is complete
     */
    void LoadCopyset(const LogicPoolID &logicPoolId,
                     const CopysetID &copysetId,
                     bool needCheckLoadFinished);
    /**
     * Check the status of the specified copyset until the copyset is loaded or an exception occurs
     * @param node: Specific copyset node
     * @return true means the load was successful, false means an exception occurred
     */
    bool CheckCopysetUntilLoadFinished(std::shared_ptr<CopysetNode> node);

    /**
     * Get the status of the copysetNodeManager loading copyset
     * @return false-copyset for not loaded, true-copyset for loaded
     */
    virtual bool LoadFinished();

 protected:
    CopysetNodeManager()
        : copysetLoader_(nullptr)
        , running_(false)
        , loadFinished_(false) {}

 private:
    /**
     * If the specified copyset does not exist, insert the copyset into the map (thread-safe)
     * @param logicPoolId:logicPool id
     * @param copysetId:copyset id
     * @param node: copysetnode to be inserted
     * @return If copyset does not exist, insert into map and return true;
     *         If copyset exists, return false.
     */
    bool InsertCopysetNodeIfNotExist(const LogicPoolID &logicPoolId,
                                     const CopysetID &copysetId,
                                     std::shared_ptr<CopysetNode> node);

    /**
     * Create a new copyset or load an existing copyset (non-thread safe)
     * @param logicPoolId:logicPool id
     * @param copysetId:copyset id
     * @param conf:Configuration peers of this copyset
     * @return Return copysetnode if created or loaded successfully, otherwise returns nullptr
     */
    std::shared_ptr<CopysetNode> CreateCopysetNodeUnlocked(
        const LogicPoolID &logicPoolId,
        const CopysetID &copysetId,
        const Configuration &conf);

 private:
    using CopysetNodeMap = std::unordered_map<GroupId,
                                              std::shared_ptr<CopysetNode>>;
    // read-write locks which protects copyset
    mutable RWLock rwLock_;
    // copyset node map
    CopysetNodeMap copysetNodeMap_;
    // copyset configuration options
    CopysetNodeOptions copysetNodeOptions_;
    // Control the number of concurrent starts of copyset
    std::shared_ptr<TaskThreadPool<>> copysetLoader_;
    // indicate whether the copyset node manager is running
    Atomic<bool> running_;
    // indicate whether the copyset node manager is loaded
    Atomic<bool> loadFinished_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_COPYSET_NODE_MANAGER_H_
