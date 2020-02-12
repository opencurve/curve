/*
 * Project: nebd
 * Created Date: Friday February 14th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_HEARTBEAT_MANAGER_H_
#define SRC_PART2_HEARTBEAT_MANAGER_H_

#include <thread>  // NOLINT
#include <atomic>
#include <memory>

#include "src/common/interrupt_sleep.h"
#include "src/part2/file_manager.h"

namespace nebd {
namespace server {

using nebd::common::InterruptibleSleeper;

struct HeartbeatManagerOption {
    // 文件心跳超时时间（单位：秒）
    uint32_t heartbeatTimeoutS;
    // 心跳超时检测线程的检测间隔（时长：毫秒）
    uint32_t checkTimeoutIntervalMs;
    // filemanager 对象指针
    NebdFileManagerPtr fileManager;
};

// 负责文件心跳超时管理
class HeartbeatManager {
 public:
    explicit HeartbeatManager(HeartbeatManagerOption option)
        : isRunning_(false)
        , heartbeatTimeoutS_(option.heartbeatTimeoutS)
        , checkTimeoutIntervalMs_(option.checkTimeoutIntervalMs)
        , fileManager_(option.fileManager) {}
    virtual ~HeartbeatManager() {}

    // 启动心跳检测线程
    virtual int Run();
    // 停止心跳检测线程
    virtual int Fini();
    // part2收到心跳后，会通过该接口更新心跳中包含的文件在内存中记录的时间戳
    // 心跳检测线程会根据该时间戳判断是否需要关闭文件
    virtual int UpdateFileTimestamp(int fd, uint64_t timestamp);

 private:
    // 心跳检测线程的函数执行体
    void CheckTimeoutFunc();
    // 判断文件是否需要close
    bool CheckNeedClosed(int fd);

 private:
    // 当前heartbeatmanager的运行状态，true表示正在运行，false标为未运行
    std::atomic<bool> isRunning_;
    // 文件心跳超时时长
    uint32_t heartbeatTimeoutS_;
    // 心跳超时检测线程的检测时间间隔
    uint32_t checkTimeoutIntervalMs_;
    // 心跳检测线程
    std::thread checkTimeoutThread_;
    // 心跳检测线程的sleeper
    InterruptibleSleeper sleeper_;
    // filemanager 对象指针
    NebdFileManagerPtr fileManager_;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_HEARTBEAT_MANAGER_H_
