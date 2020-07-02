/*
 * Project: nebd
 * Created Date: 2020-01-16
 * Author: lixiaocui
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_NEBD_SERVER_H_
#define SRC_PART2_NEBD_SERVER_H_

#include <brpc/server.h>
#include <string>
#include <memory>
#include "nebd/src/common/configuration.h"
#include "nebd/src/part2/file_manager.h"
#include "nebd/src/part2/heartbeat_manager.h"
#include "nebd/src/part2/request_executor_curve.h"

namespace nebd {
namespace server {

using ::nebd::common::Configuration;
using ::curve::client::CurveClient;

class NebdServer {
 public:
    NebdServer() {}
    virtual ~NebdServer() {}

    int Init(const std::string &confPath,
        std::shared_ptr<CurveClient> curveClient =
        std::make_shared<CurveClient>());

    int RunUntilAskedToQuit();

    int Fini();

 private:
    /**
     * @brief 从配置文件加载配置项
     * @param[in] confPath 配置文件路径
     * @return false-加载配置文件失败 true-加载配置文件成功
     */
    bool LoadConfFromFile(const std::string &confPath);

    /**
     * @brief 初始化NebdFileManager
     * @return false-初始化失败 true-初始化成功
     */
    bool InitFileManager();

    /**
     * @brief 初始化request_executor_curve
     * @return false-初始化失败 true-初始化成功
     */
    bool InitCurveRequestExecutor();

    /**
     * @brief 初始化NebdMetaFileManager
     * @return nullptr-初始化不成功 否则表示初始化成功
     */
    MetaFileManagerPtr InitMetaFileManager();

    /**
     * @brief 初始化HeartbeatManagerOption
     * @param[out] opt
     * @return false-初始化失败 true-初始化成功
     */
    bool InitHeartbeatManagerOption(HeartbeatManagerOption *opt);

    /**
     * @brief 初始化HeartbeatManager
     * @return false-初始化失败 true-初始化成功
     */
    bool InitHeartbeatManager();

    /**
     * @brief 启动brpc service
     * @return false-启动service失败 true-启动service成功
     */
    bool StartServer();

 private:
    // 配置项
    Configuration conf_;
    // NebdServer监听地址
    std::string listenAddress_;
    // NebdServer是否处于running状态
    bool isRunning_ =  false;

    // brpc server
    brpc::Server server_;
    // 用于接受和处理client端的各种请求
    std::shared_ptr<NebdFileManager> fileManager_;
    // 负责文件心跳超时处理
    std::shared_ptr<HeartbeatManager> heartbeatManager_;
    // curveclient
    std::shared_ptr<CurveClient> curveClient_;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_NEBD_SERVER_H_
