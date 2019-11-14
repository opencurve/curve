/*
 * Project: curve
 * Created Date: Monday June 10th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#ifndef SRC_CHUNKSERVER_CHUNKSERVER_METRICS_H_
#define SRC_CHUNKSERVER_CHUNKSERVER_METRICS_H_

#include <bvar/bvar.h>
#include <butil/time.h>
#include <string>
#include <unordered_map>
#include <memory>
#include <vector>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/uncopyable.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/configuration.h"

using curve::common::Uncopyable;
using curve::common::RWLock;
using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using curve::common::Configuration;

namespace curve {
namespace chunkserver {

class CopysetNodeManager;
class ChunkfilePool;
class CSDataStore;
class Trash;

template <typename Tp>
using PassiveStatusPtr = std::shared_ptr<bvar::PassiveStatus<Tp>>;

template <typename Tp>
using AdderPtr = std::shared_ptr<bvar::Adder<Tp>>;

// 使用LatencyRecorder的实现来统计读写请求的size情况
// 可以统计分位值、最大值、中位数、平均值等情况
using IOSizeRecorder = bvar::LatencyRecorder;

// io 相关的统计项
class IOMetric {
 public:
    IOMetric();
    virtual ~IOMetric();
    /**
     * 初始化 io metric
     * 主要用于曝光各metric指标
     * @param prefix: 用于bvar曝光时使用的前缀
     * @return 成功返回0，失败返回-1
     */
    int Init(const std::string& prefix);
    /**
     * IO请求到来时统计requestNum
     */
    void OnRequest();
    /**
     * IO 完成以后，记录该次IO的指标
     * 错误的io不会计入iops和bps统计
     * @param size: 此次io数据的大小
     * @param latUS: 此次io的延时
     * @param hasError: 此次io是否有错误产生
     */
    void OnResponse(size_t size, int64_t latUs, bool hasError);

 public:
    // io请求的数量
    bvar::Adder<uint64_t>    reqNum_;
    // 成功io的数量
    bvar::Adder<uint64_t>    ioNum_;
    // 失败的io个数
    bvar::Adder<uint64_t>    errorNum_;
    // 所有io的数据量
    bvar::Adder<uint64_t>    ioBytes_;
    // io的延时情况（分位值、最大值、中位数、平均值）
    bvar::LatencyRecorder    latencyRecorder_;
    // io大小的情况（分位值、最大值、中位数、平均值）
    IOSizeRecorder           sizeRecorder_;
    // 最近1秒请求的IO数量
    bvar::PerSecond<bvar::Adder<uint64_t>>    rps_;
    // 最近1秒的iops
    bvar::PerSecond<bvar::Adder<uint64_t>>    iops_;
    // 最近1秒的出错IO数量
    bvar::PerSecond<bvar::Adder<uint64_t>>    eps_;
    // 最近1秒的数据量
    bvar::PerSecond<bvar::Adder<uint64_t>>    bps_;
};

using IOMetricPtr = std::shared_ptr<IOMetric>;
class CSCopysetMetric {
 public:
    CSCopysetMetric()
        : logicPoolId_(0)
        , copysetId_(0)
        , readMetric_(nullptr)
        , writeMetric_(nullptr)
        , chunkCount_(nullptr)
        , snapshotCount_(nullptr) {}

    ~CSCopysetMetric() {}

    /**
     * 初始化copyset级别的metric统计项
     * @param logicPoolId: copyset所属逻辑池的id
     * @param copysetId: copyset的id
     * @return 成功返回0，失败返回-1
     */
    int Init(const LogicPoolID& logicPoolId, const CopysetID& copysetId);

    /**
     * 记录ReadChunk请求
     */
    void OnRequestRead();

    /**
     * 记录WriteChunk请求
     */
    void OnRequestWrite();

    /**
     * ReadChunk结束时记录该次IO指标
     * 错误的io不会计入iops和bps统计
     * @param size: 此次io数据的大小
     * @param latUS: 此次io的延时
     * @param hasError: 此次io是否有错误产生
     */
    void OnResponseRead(size_t size,
                        int64_t latUs,
                        bool hasError);

    /**
     * WriteChunk结束时记录该次IO指标
     * 错误的io不会计入iops和bps统计
     * @param size: 此次io数据的大小
     * @param latUS: 此次io的延时
     * @param hasError: 此次io是否有错误产生
     */
    void OnResponseWrite(size_t size,
                         int64_t latUs,
                         bool hasError);

    /**
     * 监控DataStore指标，主要包括chunk的数量、快照的数量等
     * @param datastore: 该copyset下的datastore指针
     */
    void MonitorDataStore(CSDataStore* datastore);

    // 下列函数用户获取各项metric 指标
    const IOMetricPtr GetReadMetric() const {
        return readMetric_;
    }

    const IOMetricPtr GetWriteMetric() const {
        return writeMetric_;
    }

    const uint32_t GetChunkCount() const {
        if (chunkCount_ == nullptr) {
            return 0;
        }
        return chunkCount_->get_value();
    }

    const uint32_t GetSnapshotCount() const {
        if (snapshotCount_ == nullptr) {
            return 0;
        }
        return snapshotCount_->get_value();
    }

 private:
    inline std::string Prefix() {
        return "copyset_"
               + std::to_string(logicPoolId_)
               + "_"
               + std::to_string(copysetId_);
    }

 private:
    // 逻辑池id
    LogicPoolID logicPoolId_;
    // copyset id
    CopysetID copysetId_;
    // 通过ChunkService进来的读io统计
    IOMetricPtr readMetric_;
    // 通过ChunkService进来的写io统计
    IOMetricPtr writeMetric_;
    // copyset上的 chunk 的数量
    PassiveStatusPtr<uint32_t> chunkCount_;
    // copyset上的 快照文件 的数量
    PassiveStatusPtr<uint32_t> snapshotCount_;
};

struct ChunkServerMetricOptions {
    bool collectMetric;
    // chunkserver的ip
    std::string ip;
    // chunkserver的端口号
    uint32_t port;
    ChunkServerMetricOptions()
        : collectMetric(false), ip("127.0.0.1"), port(8888) {}
};

using CopysetMetricPtr = std::shared_ptr<CSCopysetMetric>;
using CopysetMetricMap = std::unordered_map<GroupId, CopysetMetricPtr>;

class ChunkServerMetric : public Uncopyable {
 public:
    // 实现单例
    static ChunkServerMetric *GetInstance();

    /**
     * 初始化chunkserver统计项
     * @pa)ram option: 初始化配置项
     * @return 成功返回0，失败返回-1
     */
    int Init(const ChunkServerMetricOptions& option);

    /**
     * 释放metric资源
     * @return 成功返回0，失败返回-1
     */
    int Fini();

    /**
     * 记录ReadChunk请求
     * @param logicPoolId: 此次io操作所在的逻辑池id
     * @param copysetId: 此次io操作所在的copysetid
     */
    void OnRequestRead(const LogicPoolID& logicPoolId,
                       const CopysetID& copysetId);
    /**
     * 记录WriteChunk请求
     * @param logicPoolId: 此次io操作所在的逻辑池id
     * @param copysetId: 此次io操作所在的copysetid
     */
    void OnRequestWrite(const LogicPoolID& logicPoolId,
                        const CopysetID& copysetId);

    /**
     * ReadChunk结束时记录该次IO指标
     * 错误的io不会计入iops和bps统计
     * @param logicPoolId: 此次io操作所在的逻辑池id
     * @param copysetId: 此次io操作所在的copysetid
     * @param size: 此次io数据的大小
     * @param latUS: 此次io的延时
     * @param hasError: 此次io是否有错误产生
     */
    void OnResponseRead(const LogicPoolID& logicPoolId,
                        const CopysetID& copysetId,
                        size_t size,
                        int64_t latUs,
                        bool hasError);

    /**
     * WriteChunk结束时记录该次IO指标
     * 错误的io不会计入iops和bps统计
     * @param logicPoolId: 此次io操作所在的逻辑池id
     * @param copysetId: 此次io操作所在的copysetid
     * @param size: 此次io数据的大小
     * @param latUS: 此次io的延时
     * @param hasError: 此次io是否有错误产生
     */
    void OnResponseWrite(const LogicPoolID& logicPoolId,
                         const CopysetID& copysetId,
                         size_t size,
                         int64_t latUs,
                         bool hasError);

    /**
     * 创建指定copyset的metric
     * 如果collectMetric为false，返回0，但实际并不会创建
     * @param logicPoolId: copyset所属逻辑池的id
     * @param copysetId: copyset的id
     * @return 成功返回0，失败返回-1，如果指定metric已存在返回失败
     */
    int CreateCopysetMetric(const LogicPoolID& logicPoolId,
                            const CopysetID& copysetId);

    /**
     * 获取指定copyset的metric
     * @param logicPoolId: copyset所属逻辑池的id
     * @param copysetId: copyset的id
     * @return 成功返回指定的copyset metric，失败返回nullptr
     */
    CopysetMetricPtr GetCopysetMetric(const LogicPoolID& logicPoolId,
                                      const CopysetID& copysetId);

    /**
     * 删除指定copyset的metric
     * @param logicPoolId: copyset所属逻辑池的id
     * @param copysetId: copyset的id
     * @return 成功返回0，失败返回-1
     */
    int RemoveCopysetMetric(const LogicPoolID& logicPoolId,
                            const CopysetID& copysetId);

    /**
     * 监视chunk分配池，主要监视池中chunk的数量
     * @param chunkfilePool: ChunkfilePool的对象指针
     */
    void MonitorChunkFilePool(ChunkfilePool* chunkfilePool);

    /**
     * 监视回收站
     * @param trash: trash的对象指针
     */
    void MonitorTrash(Trash* trash);

    /**
     * 增加 leader count 计数
     */
    void IncreaseLeaderCount();

    /**
     * 减少 leader count 计数
     */
    void DecreaseLeaderCount();

    /**
     * 更新配置项数据
     * @param conf: 配置内容
     */
    void UpdateConfigMetric(common::Configuration* conf);

    // 下列函数用户获取各项metric 指标
    const IOMetricPtr GetReadMetric() const {
        return readMetric_;
    }

    const IOMetricPtr GetWriteMetric() const {
        return writeMetric_;
    }

    const uint32_t GetCopysetCount() {
        ReadLockGuard lockGuard(rwLock_);
        return copysetMetricMap_.size();
    }

    const uint32_t GetLeaderCount() const {
        if (leaderCount_ == nullptr)
            return 0;
        return leaderCount_->get_value();
    }

    const uint32_t GetTotalChunkCount() {
        uint32_t totalChunkCount = 0;
        ReadLockGuard lockGuard(rwLock_);
        for (auto& iter : copysetMetricMap_) {
            totalChunkCount += iter.second->GetChunkCount();
        }
        return totalChunkCount;
    }

    const uint32_t GetTotalSnapshotCount() {
        uint32_t totalSnapshotCount = 0;
        ReadLockGuard lockGuard(rwLock_);
        for (auto& iter : copysetMetricMap_) {
            totalSnapshotCount += iter.second->GetSnapshotCount();
        }
        return totalSnapshotCount;
    }

    const uint32_t GetChunkLeftCount() const {
        if (chunkLeft_ == nullptr)
            return 0;
        return chunkLeft_->get_value();
    }

    const uint32_t GetChunkTrashedCount() const {
        if (chunkTrashed_ == nullptr)
            return 0;
        return chunkTrashed_->get_value();
    }

 private:
    ChunkServerMetric();

    inline std::string Prefix() {
        return "chunkserver_" + option_.ip + "_" + std::to_string(option_.port);
    }

 private:
    // 初始化标志
    bool hasInited_;
    // 配置项
    ChunkServerMetricOptions option_;
    // 保护复制组metric map的读写锁
    RWLock rwLock_;
    // 通过ChunkService进来的读io统计
    IOMetricPtr readMetric_;
    // 通过ChunkService进来的写io统计
    IOMetricPtr writeMetric_;
    // leader 的数量
    AdderPtr<uint32_t> leaderCount_;
    // chunkfilepool 中剩余的 chunk 的数量
    PassiveStatusPtr<uint32_t> chunkLeft_;
    // trash 中的 chunk 的数量
    PassiveStatusPtr<uint32_t> chunkTrashed_;
    // 各复制组metric的映射表，用GroupId作为key
    CopysetMetricMap copysetMetricMap_;
    // 用于单例模式的自指指针
    static ChunkServerMetric* self_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVER_METRICS_H_
