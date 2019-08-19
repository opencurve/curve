/*
 * Project: curve
 * Created Date: 20190819
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_ALLOCSTATISTIC_ALLOC_STATISTIC_H_
#define SRC_MDS_NAMESERVER2_ALLOCSTATISTIC_ALLOC_STATISTIC_H_

#include <memory>
#include <map>
#include <string>
#include "src/mds/kvstorageclient/etcd_client.h"
#include "src/mds/common/mds_define.h"
#include "src/common/concurrent/concurrent.h"

using ::curve::mds::topology::PoolIdType;
using ::curve::common::Atomic;
using ::curve::common::Mutex;
using ::curve::common::Thread;

namespace curve {
namespace mds {
class AllocStatistic {
 public:
    /**
     * @brief UsageStatistic 构造函数
     *
     * @param[in] periodicPersistInterMs
     *            将内存中各logicalPool的segment使用量向etcd持久化的时间间隔
     * @param[in] retryInterMs 从etcd中获取指定revision的segmennt失败后的重试间隔
     * @param[in] client etcdClient
     */
    AllocStatistic(uint64_t periodicPersistInterMs, uint64_t retryInterMs,
        std::shared_ptr<EtcdClientImp> client) :
        client_(client),
        currentValueAvalible_(false),
        stop_(true),
        periodicPersistInterMs_(periodicPersistInterMs),
        retryInterMs_(retryInterMs) {}

    /**
     * @brief Init 从etcd中获取定期持久化的每个physical-pool对应的已分配的segment信息
     *             以及recycleBin中的信息
     *
     * @return 0-init成功 1-init失败
     */
    int Init();

    /**
     * @brief Run 一是获取指定revision下的所有segment,
     *            二是定期持久化内存中的统计的各logcailPool下的已分配的segment大小
     */
    void Run();

    /**
     * @brief Stop 用于等待后台线程的退出
     */
    void Stop();

    /**
     * @brief GetAllocByLogicalPool 获取指定逻辑池已分配出去的容量
     *        若还未统计结束(currentValueAvalible_=false)，使用旧值，否则使用新值
     *
     * @param[in] lid 指定逻辑池id
     * @param[out] alloc lid已分配的segment大小
     *
     * @return false表示未获取成功，true表示获取成功
     */
    virtual bool GetAllocByLogicalPool(PoolIdType lid, uint64_t *alloc);

    /**
     * @brief AllocChange 用于外部更新mds启动后segment的增减
     *
     * @param[in] changeSize segment增加或者减少量
     */
    virtual void AllocChange(PoolIdType lid, int64_t changeSize);

    /**
     * @brief UpdateChangeLock 外部使用AllocChange时需要先加锁
     *
     */
    virtual void UpdateChangeLock();

    /**
     * @brief UpdateChangeLock 外部使用AllocChange完成时时需要解锁
     *
     */
    virtual void UpdateChangeUnlock();

 private:
     /**
     * @brief CalculateSegmentAlloc 从etcd中获取指定revision的所有segment记录
     */
    void CalculateSegmentAlloc();

    /**
     * @brief PeriodicPersist
     *        定期持久化内存中的统计的各logcailPool下的已分配的segment大小
     */
    void PeriodicPersist();

     /**
     * @brief HandleResult
     *        用于处理获取指定revision的所有segment记录过程中发生错误的情况
     */
    bool HandleResult(int res);

 private:
    // etcd模块
    std::shared_ptr<EtcdClientImp> client_;

    // 当前正在统计的segment的revision
    int64_t curRevision_;

    // mds启动前最后一次持久化的值
    std::map<PoolIdType, uint64_t> existSegmentAllocValues_;

    // mds启动后重新统计的已分配的segment
    std::map<PoolIdType, uint64_t> segmentAlloc_;

    // mds启动后segment的变化
    std::map<PoolIdType, int64_t> segmentChange_;

    // 当前值是否可用
    Atomic<bool> currentValueAvalible_;

    // 出错情况下的重试间隔,单位ms
    uint64_t retryInterMs_;

    // 持久化间隔, 单位ms
    uint64_t periodicPersistInterMs_;

    // 退出当前模块
    Atomic<bool> stop_;

    // 定期持久化每个逻辑池已分配的segment大小的线程
    Thread periodicPersist_;

    // 统计指定revision下已分配segment大小的线程
    Thread calculateAlloc_;

    // 1. 该锁用于保证segmentAlloc_统计出错的情况下，
    // 重新获取revision过程中没有segment的写入
    // 2. 解决segmentChange_的并发访问
    Mutex updateLock;
};
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_ALLOCSTATISTIC_ALLOC_STATISTIC_H_

