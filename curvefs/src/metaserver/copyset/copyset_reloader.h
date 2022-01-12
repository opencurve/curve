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
 * Date: Sun 22 Aug 2021 10:40:42 AM CST
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_COPYSET_RELOADER_H_
#define CURVEFS_SRC_METASERVER_COPYSET_COPYSET_RELOADER_H_

#include <atomic>
#include <memory>
#include <string>

#include "curvefs/src/metaserver/common/types.h"
#include "curvefs/src/metaserver/copyset/copyset_node.h"
#include "src/common/concurrent/task_thread_pool.h"
#include "src/fs/local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

class CopysetNodeManager;

// Reload all existing copysets
class CopysetReloader {
 public:
    explicit CopysetReloader(CopysetNodeManager* copysetNodeManager);

    ~CopysetReloader() = default;

    bool Init(const CopysetNodeOptions& options);

    /**
     * @brief Reload all existing copysets
     */
    bool ReloadCopysets();

 private:
    bool ReloadOneCopyset(const std::string& copyset);

    void LoadCopyset(PoolId poolId, CopysetId copysetId);

    bool CheckCopysetUntilLoadFinished(CopysetNode* node);

    void WaitLoadFinish();

 private:
    CopysetNodeManager* nodeManager_;
    CopysetNodeOptions options_;

    std::unique_ptr<curve::common::TaskThreadPool<>> taskPool_;
    std::atomic<bool> running_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_COPYSET_RELOADER_H_
