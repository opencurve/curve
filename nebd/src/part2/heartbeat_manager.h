/*
 * Project: nebd
 * Created Date: Friday February 14th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_HEARTBEAT_MANAGER_H_
#define SRC_PART2_HEARTBEAT_MANAGER_H_

#include <bvar/bvar.h>
#include <thread>  // NOLINT
#include <atomic>
#include <memory>
#include <map>
#include <string>

#include "nebd/src/common/interrupt_sleep.h"
#include "nebd/src/common/rw_lock.h"
#include "nebd/src/common/stringstatus.h"
#include "nebd/src/part2/file_manager.h"
#include "nebd/src/part2/define.h"

namespace nebd {
namespace server {

using nebd::common::InterruptibleSleeper;
using nebd::common::RWLock;
using nebd::common::WriteLockGuard;
using nebd::common::ReadLockGuard;

struct HeartbeatManagerOption {
    // 文件心跳超时时间（单位：秒）
    uint32_t heartbeatTimeoutS;
    // 心跳超时检测线程的检测间隔（时长：毫秒）
    uint32_t checkTimeoutIntervalMs;
    // filemanager 对象指针
    NebdFileManagerPtr fileManager;
};

const char kNebdClientMetricPrefix[] = "nebd_client_pid_";
const char kVersion[] = "version";

struct NebdClientInfo {
    NebdClientInfo(int pid2, const std::string& version2,
                   uint64_t timeStamp2) :
                        pid(pid2), timeStamp(timeStamp2) {
        version.ExposeAs(kNebdClientMetricPrefix,
                          std::to_string(pid2) + "_version");
        version.Set(kVersion, version2);
        version.Update();
    }
    // nebd client的进程号
    int pid;
    // nebd version的metric
    nebd::common::StringStatus version;
    // 上次心跳的时间戳
    uint64_t timeStamp;
};

// 负责文件心跳超时管理
class HeartbeatManager {
 public:
    explicit HeartbeatManager(HeartbeatManagerOption option)
        : isRunning_(false)
        , heartbeatTimeoutS_(option.heartbeatTimeoutS)
        , checkTimeoutIntervalMs_(option.checkTimeoutIntervalMs)
        , fileManager_(option.fileManager) {
        nebdClientNum_.expose("nebd_client_num");
    }
    virtual ~HeartbeatManager() {}

    // 启动心跳检测线程
    virtual int Run();
    // 停止心跳检测线程
    virtual int Fini();
    // part2收到心跳后，会通过该接口更新心跳中包含的文件在内存中记录的时间戳
    // 心跳检测线程会根据该时间戳判断是否需要关闭文件
    virtual bool UpdateFileTimestamp(int fd, uint64_t timestamp);
    // part2收到心跳后，会通过该接口更新part1的时间戳
    virtual void UpdateNebdClientInfo(int pid, const std::string& version,
                                      uint64_t timestamp);
    std::map<int, std::shared_ptr<NebdClientInfo>> GetNebdClients() {
        ReadLockGuard readLock(rwLock_);
        return nebdClients_;
    }

 private:
    // 心跳检测线程的函数执行体
    void CheckTimeoutFunc();
    // 判断文件是否需要close
    bool CheckNeedClosed(NebdFileEntityPtr entity);
    // 从内存中删除已经超时的nebdClientInfo
    void RemoveTimeoutNebdClient();

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
    // nebd client的信息
    std::map<int, std::shared_ptr<NebdClientInfo>> nebdClients_;
    // nebdClient的计数器
    bvar::Adder<uint64_t> nebdClientNum_;
    // file map 读写保护锁
    RWLock rwLock_;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_HEARTBEAT_MANAGER_H_
