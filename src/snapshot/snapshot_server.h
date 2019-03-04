/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_SNAPSHOT_SNAPSHOT_SERVER_H
#define CURVE_SRC_SNAPSHOT_SNAPSHOT_SERVER_H



#include <brpc/server.h>

#include <memory>
#include <atomic>
#include <string>


#include "src/snapshot/snapshot_service.h"
#include "src/snapshot/snapshot_service_manager.h"

namespace curve {
namespace snapshotserver {

/**
 * @brief 快照转储服务器配置结构体
 */
struct SnapshotServerOption {
    // 监听地址
    std::string listenAddr;
};

class SnapshotServer {
 public:
    SnapshotServer() {}

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
     * @return 错误码
     */
    int Stop();

    /**
     * @brief 运行服务器直到ctrl+c
     */
    void RunUntilAskedToQuit();

 private:
    // brpc服务器
    std::shared_ptr<brpc::Server> server_;
    // 快照转储rpc服务实现
    std::shared_ptr<SnapshotServiceImpl> service_;
    // 快照转储rpc服务管理
    std::shared_ptr<SnapshotServiceManager> serviceManager_;

    // 转储服务器配置项
    SnapshotServerOption options_;
};

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_SNAPSHOT_SERVER_H
