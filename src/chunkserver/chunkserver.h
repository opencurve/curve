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
     * @brief 初始化Chunkserve各子模块
     *
     * @param[in] argc 命令行参数总数
     * @param[in] argv 命令行参数列表
     *
     * @return 0表示成功，非0失败
     */
    int Run(int argc, char** argv);

    /**
     * @brief 停止chunkserver，结束各子模块
     */
    void Stop();

 private:
    void InitChunkFilePoolOptions(common::Configuration *conf,
        ChunkfilePoolOptions *chunkFilePoolOptions);

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
    // copysetNodeManager_ 管理chunkserver上所有copysetNode
    CopysetNodeManager* copysetNodeManager_;

    // cloneManager_ 管理克隆任务
    CloneManager cloneManager_;

    // heartbeat_ 负责向mds定期发送心跳，并下发心跳中任务
    Heartbeat heartbeat_;

    // trash_ 定期回收垃圾站中的物理空间
    std::shared_ptr<Trash> trash_;

    // install snapshot流控
    scoped_refptr<SnapshotThrottle> snapshotThrottle_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVER_H_

