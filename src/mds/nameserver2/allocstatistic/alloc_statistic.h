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
#include <set>
#include <string>
#include "src/mds/kvstorageclient/etcd_client.h"
#include "src/mds/common/mds_define.h"
#include "src/common/concurrent/concurrent.h"

using ::curve::mds::topology::PoolIdType;
using ::curve::common::Atomic;
using ::curve::common::Mutex;
using ::curve::common::RWLock;
using ::curve::common::Thread;

namespace curve {
namespace mds {
/**
 * AllocStatistic 用于统计当前已分配出去的segment量
 * 思路
 * 统计分为两部分:
 * part1:
 *    ①统计指定revision之前的分配量
 *    ②记录mds启动以来，每个revision对应的segment分配量
 *    ③合并①和②中的数据
 * part2: 后台定期持久化part1中合并以后的数据
 *
 * 涉及到的几个map:
 * existSegmentAllocValues_: mds上次退出持久化在etcd中的数据+mds启动以来segment的变化
 * segmentAlloc_: 最终存放part1部分完成合并之后数据
 *
 * 根据当前的统计状态给外部提供segment分配量:
 * 1. 如果part1部分全部完成，从mergeMap_中获取数据
 * 2. 如果part1部分未完成，从existSegmentAllocValues_中获取数据
 */
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
        segmentAllocFromEtcdOK_(false),
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
    virtual bool GetAllocByLogicalPool(PoolIdType lid, int64_t *alloc);

    /**
     * @brief AllocSpace put segment后更新
     *
     * @param[in] lid segment所在的logicalpoolId
     * @param[in] changeSize segment增加量
     * @param[in] revison 本次变化对应的版本
     */
    virtual void AllocSpace(PoolIdType, int64_t changeSize, int64_t revision);

    /**
     * @brief DeAllocSpace delete segment后更新
     *
     * @param[in] lid segment所在的logicalpoolId
     * @param[in] changeSize segment减少量
     * @param[in] revison 本次变化对应的版本
     */
    virtual void DeAllocSpace(
        PoolIdType, int64_t changeSize, int64_t revision);

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

    /**
     * @brief DoMerge 对于每个logicalPool, 合并变化量和etcd中读取的数据
     */
    void DoMerge();

    /**
     * @brief GetLatestSegmentAllocInfo
     *        用于获取当前需要持久化到etcd中的值.
     *
     * @return 当前每个logicalPool需要持久化到etcd中的值
     */
    std::map<PoolIdType, int64_t> GetLatestSegmentAllocInfo();

    /**
     * @brief UpdateSegmentAllocByCurrrevision 合并变化量和etcd中读取的数据
     *
     * @param[in] lid logicalPoolId
     * @paran[out] 该logicalpool的segment分配量
     */
    void UpdateSegmentAllocByCurrrevision(PoolIdType lid);

    /**
     * @brief GetCurrentLogicalPools 获取当前所有的logicalPool
     *
     * @return 返回logical pool集合
     */
    std::set<PoolIdType> GetCurrentLogicalPools();

 private:
    // etcd模块
    std::shared_ptr<EtcdClientImp> client_;

    // 当前正在统计的segment的revision
    int64_t curRevision_;

    // mds启动前最后一次持久化的值
    std::map<PoolIdType, int64_t> existSegmentAllocValues_;
    RWLock existSegmentAllocValuesLock_;

    // 前期存放统计指定revision前segment的分配量
    // 后期存放合并之后的量
    std::map<PoolIdType, int64_t> segmentAlloc_;
    RWLock segmentAllocLock_;
    Atomic<bool> segmentAllocFromEtcdOK_;

    // mds启动后segment的变化
    // PoolIdType: poolId
    // std::map<int64_t, int64_t> first表示版本, second表示变化量
    std::map<PoolIdType, std::map<int64_t, int64_t>> segmentChange_;
    RWLock segmentChangeLock_;

    // segmentAlloc_中的值是否可以使用
    // 经过至少一次合并之后即可用
    Atomic<bool> currentValueAvalible_;

    // 出错情况下的重试间隔,单位ms
    uint64_t retryInterMs_;

    // 持久化间隔, 单位ms
    uint64_t periodicPersistInterMs_;

    // stop_为true的时候停止持久化线程和统计etcd中segment分配量的统计线程
    Atomic<bool> stop_;

    // 定期持久化每个逻辑池已分配的segment大小的线程
    Thread periodicPersist_;

    // 统计指定revision下已分配segment大小的线程
    Thread calculateAlloc_;
};
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_ALLOCSTATISTIC_ALLOC_STATISTIC_H_

