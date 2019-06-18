/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/12/20  Wenyu Zhou   Initial version
 */

#ifndef SRC_CHUNKSERVER_HEARTBEAT_H_
#define SRC_CHUNKSERVER_HEARTBEAT_H_

#include <braft/node_manager.h>
#include <braft/node.h>                  // NodeImpl

#include <map>
#include <vector>
#include <string>
#include <atomic>
#include <memory>
#include <thread>  //NOLINT

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "proto/heartbeat.pb.h"

namespace curve {
namespace chunkserver {

using HeartbeatRequest  = curve::mds::heartbeat::ChunkServerHeartbeatRequest;
using HeartbeatResponse = curve::mds::heartbeat::ChunkServerHeartbeatResponse;
using ConfigChangeInfo  = curve::mds::heartbeat::ConfigChangeInfo;
using CopySetConf       = curve::mds::heartbeat::CopySetConf;
using CandidateError    = curve::mds::heartbeat::CandidateError;
using TaskStatus        = butil::Status;
using CopysetNodePtr    = std::shared_ptr<CopysetNode>;

static std::atomic<uint64_t> readCount;
static std::atomic<uint64_t> writeCount;
static std::atomic<uint64_t> readBytes;
static std::atomic<uint64_t> writeBytes;

static uint64_t GetAtomicUint64(void* arg) {
    std::atomic<uint64_t>* v = (std::atomic<uint64_t> *)arg;
    return v->load(std::memory_order_acquire);
}

/**
 * 心跳子系统选项
 */
struct HeartbeatOptions {
    ChunkServerID           chunkserverId;
    std::string             chunkserverToken;
    std::string             storeUri;
    std::string             mdsIp;
    std::string             ip;
    uint16_t                mdsPort;
    uint16_t                port;
    uint32_t                interval;
    uint32_t                timeout;
    CopysetNodeManager*     copysetNodeManager;

    std::shared_ptr<LocalFileSystem> fs;
};

struct ChunkServerMetric {
    // ChunkServer读请求累计数
    std::shared_ptr<bvar::PassiveStatus<uint64_t>> readCnt;
    // ChunkServer写请求累计数
    std::shared_ptr<bvar::PassiveStatus<uint64_t>> writeCnt;
    // ChunkServer读请求累计字节数
    std::shared_ptr<bvar::PassiveStatus<uint64_t>> readBytes;
    // ChunkServer写请求累计字节数
    std::shared_ptr<bvar::PassiveStatus<uint64_t>> writeBytes;
    // ChunkServer读请求每秒计数
    std::shared_ptr<bvar::PerSecond<bvar::PassiveStatus<uint64_t>>> readIops;
    // ChunkServer写请求每秒计数
    std::shared_ptr<bvar::PerSecond<bvar::PassiveStatus<uint64_t>>> writeIops;
    // ChunkServer读请求每秒字节数
    std::shared_ptr<bvar::PerSecond<bvar::PassiveStatus<uint64_t>>> readBps;
    // ChunkServer写请求每秒字节数
    std::shared_ptr<bvar::PerSecond<bvar::PassiveStatus<uint64_t>>> writeBps;

    void Init(std::string ip, uint16_t port) {
        std::string prefix = "chunkserver_" + ip + "_" + std::to_string(port);

        readCnt = std::make_shared<bvar::PassiveStatus<uint64_t>>(
                    prefix, "read_count", GetAtomicUint64, &readCount);
        writeCnt = std::make_shared<bvar::PassiveStatus<uint64_t>>(
                    prefix, "write_count", GetAtomicUint64, &writeCount);
        readBytes = std::make_shared<bvar::PassiveStatus<uint64_t>>(
                        prefix, "read_bytes", GetAtomicUint64, &readBytes);
        writeBytes = std::make_shared<bvar::PassiveStatus<uint64_t>>(
                        prefix, "write_bytes", GetAtomicUint64, &writeBytes);

        readIops =
            std::make_shared<bvar::PerSecond<bvar::PassiveStatus<uint64_t>>>(
                        prefix, "read_iops", readCnt.get());
        writeIops =
            std::make_shared<bvar::PerSecond<bvar::PassiveStatus<uint64_t>>>(
                        prefix, "write_iops", writeCnt.get());
        readBps =
            std::make_shared<bvar::PerSecond<bvar::PassiveStatus<uint64_t>>>(
                        prefix, "read_bps", readBytes.get());
        writeBps =
            std::make_shared<bvar::PerSecond<bvar::PassiveStatus<uint64_t>>>(
                        prefix, "write_bps", writeBytes.get());
    }
};

/**
 * 心跳子系统处理模块
 */
class Heartbeat {
 public:
    Heartbeat() {}
    ~Heartbeat() {}

    /**
     * @brief 初始化心跳子系统
     * @param[in] options 心跳子系统选项
     * @return 0:成功，非0失败
     */
    int Init(const HeartbeatOptions& options);

    /**
     * @brief 清理心跳子系统
     * @return 0:成功，非0失败
     */
    int Fini();

    /**
     * @brief 启动心跳子系统
     * @return 0:成功，非0失败
     */
    int Run();

 private:
    /**
     * @brief 停止心跳子系统
     * @return 0:成功，非0失败
     */
    int Stop();

    /*
     * 心跳工作线程
     */
    static void HeartbeatWorker(Heartbeat *heartbeat);

    /*
     * 获取Chunkserver存储空间信息
     */
    int GetFileSystemSpaces(size_t* capacity, size_t* free);

    /*
     * 构建心跳消息的Copyset信息项
     */
    int BuildCopysetInfo(curve::mds::heartbeat::CopySetInfo* info,
                         CopysetNodePtr copyset);

    /*
     * 构建心跳请求
     */
    int BuildRequest(HeartbeatRequest* request);

    /*
     * 发送心跳消息
     */
    int SendHeartbeat(const HeartbeatRequest& request,
                      HeartbeatResponse* response);

    /*
     * 执行心跳任务
     */
    int ExecTask(const HeartbeatResponse& response);

    /*
     * 等待下一个心跳时间点
     */
    void WaitForNextHeartbeat();

    /*
     * 输出心跳请求信息
     */
    void DumpHeartbeatRequest(const HeartbeatRequest& request);

    /*
     * 输出心跳回应信息
     */
    void DumpHeartbeatResponse(const HeartbeatResponse& response);

    /*
     * 清理复制组实例及持久化数据
     */
    TaskStatus PurgeCopyset(LogicPoolID poolId, CopysetID copysetId);

    /*
     * 更新ChunkServer性能复合metric
     */
    void UpdateChunkserverPerfMetric();

 private:
    // 心跳线程
    std::unique_ptr<std::thread> hbThread_;

    // 控制心跳模块运行或停止
    std::atomic<bool> toStop_;

    // Copyset管理模块
    CopysetNodeManager* copysetMan_;

    // ChunkServer目录
    std::string storePath_;

    // 心跳选项
    HeartbeatOptions options_;

    // MDS的地址
    butil::EndPoint mdsEp_;

    // ChunkServer本身的地址
    butil::EndPoint csEp_;

    // chunkserver metric统计, 由各copyset聚合而成
    ChunkServerMetric chunkserverMetric;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_HEARTBEAT_H_

