/*
 * Project: curve
 * Created Date: Thur May 9th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_CHUNKSERVER_H_
#define SRC_CHUNKSERVER_CHUNKSERVER_H_

#include <string>
#include "src/common/configuration.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/heartbeat.h"
#include "src/chunkserver/clone_manager.h"
#include "src/chunkserver/register.h"

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

    int GetChunkServerMetaFromLocal(const std::string &storeUri,
        const std::string &metaUri,
        const std::shared_ptr<LocalFileSystem> &fs,
        ChunkServerMetadata *metadata);

    int ReadChunkServerMeta(const std::shared_ptr<LocalFileSystem> &fs,
        const std::string &metaUri, ChunkServerMetadata *metadata);

 private:
    // false-停止运行
    volatile bool toStop_;

    // chunkserver包含的模块
    CopysetNodeManager copysetNodeManager_;
    CloneManager cloneManager_;
    Heartbeat heartbeat_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVER_H_

