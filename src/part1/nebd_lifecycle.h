/*
 * Project: nebd
 * File Created: 2019-10-08
 * Author: hzchenwei7
 * Copyright (c) 2018 NetEase
 */

#ifndef SRC_PART1_NEBD_LIFECYCLE_H_
#define SRC_PART1_NEBD_LIFECYCLE_H_

#include <string>
#include <atomic>
#include <thread>  // NOLINT
#include "src/common/configuration.h"

const uint32_t kCmdLinePathBufLength = 32;
const uint32_t kCmdLineBufLength = 2048;
const uint32_t kUuidLength = 36;
const uint32_t kMetaFileBufLength = 2048;

namespace nebd {
namespace client {
typedef struct LifeCycleOptions {
    std::string part2ProcName;
    std::string part2ProcPath;
    std::string part2Addr;
    uint32_t part2KillCheckRetryTimes;
    uint32_t part2KillCheckRetryIntervalUs;
    std::string qemuProcName;
    std::string lockFile;
    std::string metadataPrefix;
    uint32_t part2StartRetryTimes;
    uint32_t part2StartRetryIntervalUs;
    uint32_t connectibleCheckTimes;
    uint32_t connectibleCheckIntervalUs;
    uint32_t portGetRetryTimes;
    uint32_t portGetRetryIntervalUs;
    uint32_t heartbeatIntervalUs;
    uint32_t rpcRetryTimes;
    uint32_t rpcRetryIntervalUs;
    uint32_t rpcRetryMaxIntervalUs;
    uint32_t rpcTimeoutMs;
} LifeCycleOptions;

// 负责生命周期管理服务
class LifeCycleManager {
 public:
    LifeCycleManager() {
        part2Port_ = 0;
        pidPart1_ = -1;
        pidPart2_ = -1;
        qemuUUID_ = "";
        isHeartbeatThreadStart_ = false;
        heartbeatThread_ = nullptr;
        shouldHeartbeatStop_ = true;
    }
    ~LifeCycleManager() {}

    /**
     * @brief 启动生命周期管理服务，检测part2是否拉起，拉起part2并获取端口信息
     * @param conf: 配置信息
     * @return 执行成功返回0，失败返回-1
     */
    int Start(common::Configuration *conf);
    /**
     * @brief 停止声明周期管理服务，退出心跳线程，kill part2
     * @param void
     * @return void
     */
    void Stop();
    /**
     * @brief 获取part2的地址
     * @param void
     * @return 返回获取的part2的地址
     */
    std::string GetPart2Addr();
    /**
     * @brief 启动心跳服务
     * @param void
     * @return void
     */
    void StartHeartbeat();
    // for test
    /**
     * @brief part2进程是否存活
     * @param void
     * @return 如果存活返回true，如果不存活返回false
     */
    bool IsPart2Alive();
    /**
     * @brief 心跳服务线程是否启动
     * @param void
     * @return 如果启动返回true，如果未启动返回false
     */
    bool IsHeartbeatThreadStart();
    /**
     * @brief kill part2服务
     * @param void
     * @return 执行成功返回0，失败返回-1
     */
    int KillPart2();

 private:
    int LoadConf(common::Configuration *conf);
    char *SkipSpace(char *in, int *offset, int len);
    char *SkipNoSpace(char *in, int *offset, int len);
    /**
     * @brief 根据pid和进程名，从cmdline中解析uuid
     * @param pid: 进程id
     * @param procName: 进程名字
     * @param uuid: 返回从cmdline中获取的uuid
     * @return 执行成功返回0，失败返回-1
     */
    int getUuid(pid_t pid, const char *procName, char *uuid);
    /**
     * @brief 检查指定uuid和进程名的进程是否活着，如果进程活着，pid返回进程id
     * @param uuid: 
     * @param procName: 进程名字
     * @param[out] procExist: 进程是否存活
     * @param[out] pid：进程id
     * @return 执行成功返回0，失败返回-1
     */
    int CheckProcAlive(const char* uuid, const char* procName,
                                bool *procExist, pid_t *pid);
    /**
     * @brief 从metafile中读取port信息
     * @param[out] port：返回读到的port信息
     * @return 执行成功返回0，失败返回-1
     */
    int GetPortFromPart2(uint32_t *port);
    /**
     * @brief 从metafile中读取port信息，如果读取失败多次读取
     * @param fd：读取port时需要加文件锁，文件锁的fd
     * @param[out] port：返回读到的port信息
     * @return 执行成功返回0，失败返回-1
     */
    int GetPortWithRetry(int fd, uint32_t *port);
    /**
     * @brief 检测part2是否可联通
     * @param void
     * @return 可联通返回true，否则返回false
     */
    bool IsPart2Connectible();
    /**
     * @brief 检测part2是否可联通，重复多次检查不可联通才返回失败
     * @param retryTimes: 重试次数
     * @param retryIntervalUs：重试间隔
     * @return 可联通返回true，否则返回false
     */
    bool IsPart2ConnectibleWithRetry(uint32_t retryTimes,
                                    uint32_t retryIntervalUs);
    /**
     * @brief 清理part1和part2共用的metadate文件
     * @param void
     * @return void
     */
    void CleanMetadataFile();
    /**
     * @brief 拉起part2服务，只负责拉起，不负责检测是否拉起成功
     * @param void
     * @return void
     */
    void StartPart2();
    /**
     * @brief 拉起part2服务，拉起成功后，读取port信息
     * @param fd：读取port时需要加文件锁，文件锁的fd
     * @param[out] port: 返回读取到的port信息
     * @return 执行成功返回0，失败返回-1
     */
    int StartPart2AndGetPortRetry(int fd, uint32_t *port);
    /**
     * @brief 心跳线程的执行函数
     * @param void
     * @return void
     */
    void HeartbeatThreadFunc();

 private:
    // 从配置文件中读取到的声明周期管理所需要的配置字段
    LifeCycleOptions lifeCycleOptions_;
    // part2的服务的port
    uint32_t part2Port_;
    // part1的pid
    pid_t pidPart1_;
    // part2的pid
    pid_t pidPart2_;
    // qemu的UUID
    std::string qemuUUID_;
    // heartbeat线程是否启动
    std::atomic<bool> isHeartbeatThreadStart_;
    // heartbeat的线程
    std::thread *heartbeatThread_;
    // 控制heartbeat线程退出的标志位
    std::atomic<bool> shouldHeartbeatStop_;
};
}  // namespace client
}  // namespace nebd
#endif  // SRC_PART1_NEBD_LIFECYCLE_H_
