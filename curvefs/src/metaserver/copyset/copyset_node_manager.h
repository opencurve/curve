/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Fri Aug  6 17:10:54 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_COPYSET_NODE_MANAGER_H_
#define CURVEFS_SRC_METASERVER_COPYSET_COPYSET_NODE_MANAGER_H_

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "curvefs/src/metaserver/common/types.h"
#include "curvefs/src/metaserver/copyset/copyset_node.h"
#include "curvefs/src/metaserver/copyset/types.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

class CopysetNodeManager {
    friend class CopysetReloader;

 public:
    static CopysetNodeManager& GetInstance() {
        static CopysetNodeManager instance;
        return instance;
    }

 public:
    virtual ~CopysetNodeManager() = default;

    bool Init(const CopysetNodeOptions& options);

    bool Start();

    bool Stop();

    virtual CopysetNode* GetCopysetNode(PoolId poolId, CopysetId copysetId);

    virtual std::shared_ptr<CopysetNode> GetSharedCopysetNode(
        PoolId poolId, CopysetId copysetId);

    /**
     * @return 0: not exist; 1: key exist and peers are exactly same;
     * -1: key exist but peers are not exactly same
     */
    int IsCopysetNodeExist(const CreateCopysetRequest::Copyset& copyset);

    bool CreateCopysetNode(PoolId poolId, CopysetId copysetId,
                           const braft::Configuration& conf,
                           bool checkLoadFinish = true);

    bool DeleteCopysetNode(PoolId poolId, CopysetId copysetId);

    virtual bool PurgeCopysetNode(PoolId poolId, CopysetId copysetId);

    void GetAllCopysets(std::vector<CopysetNode*>* nodes) const;

    virtual bool IsLoadFinished() const;

 public:
    CopysetNodeManager()
        : options_(),
          running_(false),
          loadFinished_(false),
          lock_(),
          copysets_() {}

 public:
    /**
     * @brief Add raft related services to server
     */
    void AddService(brpc::Server* server, const butil::EndPoint& listenAddr);

 private:
    bool DeleteCopysetNodeInternal(PoolId poolId, CopysetId copysetId,
                                   bool removeData);

 private:
    using CopysetNodeMap =
        std::unordered_map<braft::GroupId, std::shared_ptr<CopysetNode>>;

    CopysetNodeOptions options_;

    std::atomic<bool> running_;

    // whether copyset is loaded finished, manager will reject create copyset
    // request if load unfinished
    std::atomic<bool> loadFinished_;

    // protected copysets_
    mutable RWLock lock_;

    CopysetNodeMap copysets_;

    CopysetTrash trash_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_COPYSET_NODE_MANAGER_H_
