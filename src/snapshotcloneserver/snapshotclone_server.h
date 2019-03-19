/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_SNAPSHOTCLONE_SERVER_H_
#define SRC_SNAPSHOTCLONESERVER_SNAPSHOTCLONE_SERVER_H_



#include <brpc/server.h>

#include <memory>
#include <atomic>
#include <string>


#include "src/snapshotcloneserver/snapshotclone_service.h"
#include "src/snapshotcloneserver/snapshot/snapshot_service_manager.h"
#include "src/snapshotcloneserver/clone/clone_service_manager.h"

namespace curve {
namespace snapshotcloneserver {

/**
 * @brief 快照转储服务器配置结构体
 */
struct SnapshotServerOption {
    // 监听地址
    std::string listenAddr;
};

class SnapshotCloneServer {
 public:
    SnapshotCloneServer() {}

    /**
     * @brief 初始化
     *
     * @return 错误码
     */
    int Init();

    /**
     * @brief 启动快照转储服务
     *
     * @return 错误码
     */
    int Start();

    /**
     * @brief 停止快照转储服务器
     *
     */
    void Stop();

    /**
     * @brief 运行服务器直到ctrl+c
     */
    void RunUntilAskedToQuit();

 private:
    // brpc服务器
    std::shared_ptr<brpc::Server> server_;
    // 快照转储rpc服务实现
    std::shared_ptr<SnapshotCloneServiceImpl> service_;
    // 快照转储rpc服务管理
    std::shared_ptr<SnapshotServiceManager> snapshotServiceManager_;
    std::shared_ptr<CloneServiceManager> cloneServiceManager_;

    // 转储服务器配置项
    SnapshotServerOption options_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOTCLONE_SERVER_H_
