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
 * Created Date: Thur May 9th 2019
 * Author: lixiaocui
 */

#ifndef SRC_CHUNKSERVER_CHUNKSERVER_H_
#define SRC_CHUNKSERVER_CHUNKSERVER_H_

#include <string>
#include <memory>
#include "src/common/configuration.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/heartbeat.h"
#include "src/chunkserver/clone_manager.h"
#include "src/chunkserver/register.h"
#include "src/chunkserver/trash.h"
#include "src/chunkserver/chunkserver_metrics.h"
#include "src/chunkserver/concurrent_apply/concurrent_apply.h"

using ::curve::chunkserver::concurrent::ConcurrentApplyOption;

namespace curve {
namespace chunkserver {
class ChunkServer {
 public:
    /**
     * @brief Initialize the Chunkserve sub-modules
     *
     * @param[in] argc Total number of command line parameters
     * @param[in] argv List of command line parameters
     *
     * @return 0 for success, non-0 for failure
     */
    int Run(int argc, char** argv);

    /**
     * @brief Stop the chunkserver and the submodules
     */
    void Stop();

 private:
    void InitChunkFilePoolOptions(common::Configuration *conf,
        FilePoolOptions *chunkFilePoolOptions);

    void InitWalFilePoolOptions(common::Configuration *conf,
        FilePoolOptions *walPoolOption);

    void InitConcurrentApplyOptions(common::Configuration *conf,
        ConcurrentApplyOption *concurrentApplyOption);

    void InitCopysetNodeOptions(common::Configuration *conf,
        CopysetNodeOptions *copysetNodeOptions);

    void InitCopyerOptions(common::Configuration *conf,
        CopyerOptions *copyerOptions);

    void InitCloneOptions(common::Configuration *conf,
        CloneOptions *cloneOptions);

    void InitHeartbeatOptions(common::Configuration *conf,
        HeartbeatOptions *heartbeatOptions);

    void InitRegisterOptions(common::Configuration *conf,
        RegisterOptions *registerOptions);

    void InitTrashOptions(common::Configuration *conf,
        TrashOptions *trashOptions);

    void InitMetricOptions(common::Configuration *conf,
        ChunkServerMetricOptions *metricOptions);

    void LoadConfigFromCmdline(common::Configuration *conf);

    int GetChunkServerMetaFromLocal(const std::string &storeUri,
        const std::string &metaUri,
        const std::shared_ptr<LocalFileSystem> &fs,
        ChunkServerMetadata *metadata);

    int ReadChunkServerMeta(const std::shared_ptr<LocalFileSystem> &fs,
        const std::string &metaUri, ChunkServerMetadata *metadata);

 private:
    // copysetNodeManager_ manages all copysetNodes on the chunkserver
    CopysetNodeManager* copysetNodeManager_;

    // cloneManager_ manages cloning tasks
    CloneManager cloneManager_;

    // heartbeat_ is responsible for sending regular heartbeats to mds and issuing tasks in the heartbeat
    Heartbeat heartbeat_;

    // trash_ recycles the physical space in the trash on a regular basis
    std::shared_ptr<Trash> trash_;

    // install snapshot Throttle
    scoped_refptr<SnapshotThrottle> snapshotThrottle_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVER_H_

