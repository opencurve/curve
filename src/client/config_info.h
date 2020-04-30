/*
 * Project: curve
 * File Created: Saturday, 29th December 2018 3:50:45 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_CONFIG_INFO_H_
#define SRC_CLIENT_CONFIG_INFO_H_

#include <stdint.h>
#include <string>
#include <vector>

/**
 * log的基本配置信息
 * @logLevel: 是log打印等级
 * @logPath: log打印位置
 */
typedef struct LogInfo {
    int         logLevel;
    std::string logPath;
    LogInfo() {
        logLevel = 2;
    }
} LogInfo_t;

/**
 * in flight IO控制信息
 * @fileMaxInFlightRPCNum: 为一个文件中最大允许的inflight IO数量
 */
typedef struct InFlightIOCntlInfo {
    uint64_t    fileMaxInFlightRPCNum;
    InFlightIOCntlInfo() {
        fileMaxInFlightRPCNum = 2048;
    }
} InFlightIOCntlInfo_t;

/**
 * mds client的基本配置
 * @mdsMaxRetryMS: rpc重试总时间
 * @mdsRPCTimeoutMs: 与mds通信的rpc超时时间
 * @mdsMaxRPCTimeoutMS: 与mds通信的rpc最大超时时间，因为与mds通信时
 *                      如果rpc超时会触发指数退避，增加rpc超时时间，最大超时时间
 *                      不能大于mdsMaxRPCTimeoutMS这个值。
 * @mdsRPCRetryIntervalUS: rpc重试间隔时间，当rpc非超时失败时，如果下一次重试还是在当前
 *                      mds重试，不会立即重试请求，而是选择睡眠一小段时间之后再去发RPC。
 * @mdsMaxFailedTimesBeforeChangeMDS: 如果重试的rpc在一个mds节点上连续失败超过该值
 *                       就需要主动触发切换mds再重试。
 * @metaaddrvec: mds server地址，存放mds集群的多个地址信息
 */
typedef struct MetaServerOption {
    uint64_t mdsMaxRetryMS;
    uint64_t mdsRPCTimeoutMs;
    uint64_t mdsMaxRPCTimeoutMS;
    uint32_t mdsRPCRetryIntervalUS;
    uint32_t mdsMaxFailedTimesBeforeChangeMDS;
    std::vector<std::string> metaaddrvec;
    MetaServerOption() {
        mdsMaxRetryMS = 8000;
        mdsMaxRPCTimeoutMS = 2000;
        mdsRPCTimeoutMs = 500;
        mdsRPCRetryIntervalUS = 50000;
        mdsMaxFailedTimesBeforeChangeMDS = 5;
    }
} MetaServerOption_t;

/**
 * 租约基本配置
 * @mdsRefreshTimesPerLease: 一个租约内续约次数，client与mds之间通过租约保持心跳
 *                           如果双方约定租约有效期为10s，那么client会在这10s内
 *                           发送mdsRefreshTimesPerLease次心跳，如果连续失败，
 *                           那么client认为当前mds存在异常，会阻塞后续的IO，直到
 *                           续约成功。
 */
typedef struct LeaseOption {
    uint32_t mdsRefreshTimesPerLease;
    LeaseOption() {
        mdsRefreshTimesPerLease = 5;
    }
} LeaseOption_t;

/**
 * rpc超时，判断是否unstable的参数
 * @maxStableChunkServerTimeoutTimes:
 *     一个chunkserver连续超时请求的阈值, 超过之后会检查健康状态，
 *     如果不健康，则标记为unstable
 * @checkHealthTimeoutMS:
 *     检查chunkserver是否健康的http请求超时时间
 * @serverUnstableThreashold:
 *     一个server上超过serverUnstableThreashold个chunkserver都标记为unstable，
 *     整个server上的所有chunkserver都标记为unstable
 */
struct ChunkServerUnstableOption {
    uint32_t maxStableChunkServerTimeoutTimes{64};
    uint32_t checkHealthTimeoutMS{100};
    uint32_t serverUnstableThreshold{3};
};

/**
 * 发送失败的chunk request处理
 * @chunkserverOPMaxRetry: 最大重试次数，一个RPC下发到底层chunkserver，最大允许的失败
 *                         次数，超限之后会向上返回用户。
 * @chunkserverOPRetryIntervalUS: 相隔多久再重试，一个rpc失败之后client会根据其返回
 *                          状态决定是否需要睡眠一段时间再重试，目前除了
 *                          TIMEOUT、REDIRECTED，这两种返回值，其他返回值都是需要
 *                          先睡眠一段时间再重试。
 * @chunkserverRPCTimeoutMS: 为每个rpc发送时，其rpc controller配置的超时时间
 * @chunkserverMaxRPCTimeoutMS: 在底层chunkserver返回TIMEOUT时，说明当前请求在底层
 *                          无法及时得到处理，原因可能是底层的排队任务太多，这时候如果
 *                          以相同的rpc 超时时间再去发送请求，很有可能最后还是超时，
 *                          所以为了避免底层处理请求时，rpc在client一侧已经超时的这种
 *                          状况，为rpc超时时间增加了指数退避逻辑，超时时间会逐渐增加，
 *                          最大不能超过该值。
 * @chunkserverMaxRetrySleepIntervalUS: 在底层返回OVERLOAD时，表明当前chunkserver
 *                          压力过大，这时候睡眠时间会进行指数退避，睡眠时间会加长，这样
 *                          能够保证client的请求不会将底层chunkserver打满，但是睡眠时间
 *                          最长不能超过该值。
 * @chunkserverMaxStableTimeoutTimes: 一个chunkserver连续超时请求的阈值, 超过之后
 *                          会标记为unstable。因为一个chunkserver所在的server如果宕机
 *                          那么发向该chunkserver的请求都会超时，如果同一个chunkserver
 *                          的rpc连续超时超过该阈值，那么client就认为这个chunkserver
 *                          所在的server可能宕机了，就将该server上的所有leader copyset
 *                          标记为unstable，促使其下次发送rpc前，先去getleader。
 * @chunkserverMinRetryTimesForceTimeoutBackoff: 当一个请求重试次数超过阈值时，还在重试
 *                          使其超时时间进行指数退避
 * @chunkserverMaxRetryTimesBeforeConsiderSuspend: rpc重试超过这个次数后被认为是悬挂IO，
 *                          因为发往chunkserver底层的rpc重试次数非常大，如果一个rpc连续
 *                          失败超过该阈值的时候，可以认为当前IO处于悬挂状态，通过metric
 *                          向上报警。
 */
typedef struct FailureRequestOption {
    uint32_t chunkserverOPMaxRetry;
    uint64_t chunkserverOPRetryIntervalUS;
    uint64_t chunkserverRPCTimeoutMS;
    uint64_t chunkserverMaxRPCTimeoutMS;
    uint64_t chunkserverMaxRetrySleepIntervalUS;
    uint64_t chunkserverMinRetryTimesForceTimeoutBackoff;
    uint64_t chunkserverMaxRetryTimesBeforeConsiderSuspend;
    ChunkServerUnstableOption chunkserverUnstableOption;

    FailureRequestOption() {
        chunkserverOPMaxRetry = 3;
        chunkserverOPRetryIntervalUS = 200;
        chunkserverRPCTimeoutMS = 1000;
        chunkserverMaxRPCTimeoutMS = 64000;
        chunkserverMaxRetrySleepIntervalUS = 64 * 1000 * 1000;
        chunkserverMinRetryTimesForceTimeoutBackoff = 5;
        chunkserverMaxRetryTimesBeforeConsiderSuspend = 20;
    }
} FailureRequestOption_t;

/**
 * 发送rpc给chunkserver的配置
 * @chunkserverEnableAppliedIndexRead: 是否开启使用appliedindex read
 * @inflightOpt: 一个文件向chunkserver发送请求时的inflight 请求控制配置
 * @failRequestOpt: rpc发送失败之后，需要进行rpc重试的相关配置
 */
typedef struct IOSenderOption {
    bool chunkserverEnableAppliedIndexRead;
    InFlightIOCntlInfo_t inflightOpt;
    FailureRequestOption_t failRequestOpt;
} IOSenderOption_t;

/**
 * scheduler模块基本配置信息，schedule模块是用于分发用户请求，每个文件有自己的schedule
 * 线程池，线程池中的线程各自配置一个队列
 * @scheduleQueueCapacity: schedule模块配置的队列深度
 * @scheduleThreadpoolSize: schedule模块线程池大小
 */
typedef struct RequestScheduleOption {
    uint32_t scheduleQueueCapacity;
    uint32_t scheduleThreadpoolSize;
    IOSenderOption_t ioSenderOpt;
    RequestScheduleOption() {
        scheduleQueueCapacity = 1024;
        scheduleThreadpoolSize = 2;
    }
} RequestScheduleOption_t;

/**
 * metaccache模块配置信息
 * @metacacheGetLeaderRetry: 获取leader重试次数，一个rpc发送到chunkserver之前需要先
 *                           获取当前copyset的leader，如果metacache中没有这个信息，
 *                           就向copyset的peer发送getleader请求，如果getleader失败，
 *                           需要重试，最大重试次数为该值。
 * @metacacheRPCRetryIntervalUS: 如上所述，如果getleader请求失败，会发起重试，但是并
 *                           不会立即进行重试，而是选择先睡眠一段时间在重试。该值代表
 *                            睡眠长度。
 * @metacacheGetLeaderRPCTimeOutMS: 发送getleader rpc请求的rpc controller最大超时时间
 * @metacacheGetLeaderBackupRequestMS: 因为一个copyset有三个或者更多的peer，getleader
 *                            会以backuprequest的方式向这些peer发送rpc，在brpc内部
 *                            会串行发送，如果第一个请求超过一定时间还没返回，就直接向
 *                            下一个peer发送请求，而不用等待上一次请求返回或超时，这个触发
 *                            backup request的时间就为该值。
 * @metacacheGetLeaderBackupRequestLbName: 为getleader backup rpc
 *                            选择底层服务节点的策略
 */
typedef struct MetaCacheOption {
    uint32_t metacacheGetLeaderRetry;
    uint32_t metacacheRPCRetryIntervalUS;
    uint32_t metacacheGetLeaderRPCTimeOutMS;
    uint32_t metacacheGetLeaderBackupRequestMS;
    std::string metacacheGetLeaderBackupRequestLbName;
    MetaCacheOption() {
        metacacheGetLeaderRetry = 3;
        metacacheRPCRetryIntervalUS = 500;
        metacacheGetLeaderRPCTimeOutMS = 1000;
        metacacheGetLeaderBackupRequestMS = 100;
        metacacheGetLeaderBackupRequestLbName = "rr";
    }
} MetaCacheOption_t;

/**
 * IO 拆分模块配置信息
 * @fileIOSplitMaxSizeKB: 用户下发IO大小client没有限制，但是client会将用户的IO进行拆分，
 *                        发向同一个chunkserver的请求锁携带的数据大小不能超过该值。
 */
typedef struct IOSplitOPtion {
    uint64_t  fileIOSplitMaxSizeKB;
    IOSplitOPtion() {
        fileIOSplitMaxSizeKB = 64;
    }
} IOSplitOPtion_t;

/**
 * 线程隔离任务队列配置信息
 * 线程隔离主要是为了上层做异步接口调用时，直接将其调用任务推到线程池中而不是让其阻塞到放入
 * 分发队列线程池。
 * @isolationTaskQueueCapacity: 隔离线程池的队列深度
 * @isolationTaskThreadPoolSize: 隔离线程池容量
 */
typedef struct TaskThreadOption {
    uint64_t    isolationTaskQueueCapacity;
    uint32_t    isolationTaskThreadPoolSize;
    TaskThreadOption() {
        isolationTaskQueueCapacity = 500000;
        isolationTaskThreadPoolSize = 1;
    }
} TaskThreadOption_t;

/**
 * IOOption存储了当前io 操作所需要的所有配置信息
 */
typedef struct IOOption {
    IOSplitOPtion_t         ioSplitOpt;
    IOSenderOption_t        ioSenderOpt;
    MetaCacheOption_t       metaCacheOpt;
    TaskThreadOption_t      taskThreadOpt;
    RequestScheduleOption_t reqSchdulerOpt;
} IOOption_t;

/**
 * client一侧常规的共同的配置信息
 * @mdsRegisterToMDS: 是否向mds注册client信息，因为client需要通过dummy server导出
 *                    metric信息，为了配合普罗米修斯的自动服务发现机制，会将其监听的
 *                    ip和端口信息发送给mds。
 * @turnOffHealthCheck: 是否关闭健康检查
 */
typedef struct CommonConfigOpt {
    bool mdsRegisterToMDS{false};
    bool turnOffHealthCheck{false};
} CommonConfigOpt_t;

/**
 * ClientConfigOption是外围快照系统需要设置的配置信息
 */
typedef struct ClientConfigOption {
    LogInfo_t                  loginfo;
    IOOption_t                 ioOpt;
    CommonConfigOpt_t          commonOpt;
    MetaServerOption_t         metaServerOpt;
} ClientConfigOption_t;

/**
 * FileServiceOption是QEMU侧总体配置信息
 */
typedef struct FileServiceOption {
    LogInfo_t                 loginfo;
    IOOption_t                ioOpt;
    LeaseOption_t             leaseOpt;
    CommonConfigOpt_t         commonOpt;
    MetaServerOption_t        metaServerOpt;
} FileServiceOption_t;

#endif  // SRC_CLIENT_CONFIG_INFO_H_
