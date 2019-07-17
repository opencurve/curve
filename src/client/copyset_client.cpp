/*
 * Project: curve
 * Created Date: 18-9-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/client/copyset_client.h"

#include <glog/logging.h>
#include <unistd.h>
#include <memory>
#include <utility>

#include "src/client/request_sender.h"
#include "src/client/metacache.h"
#include "src/client/client_config.h"
#include "src/client/request_scheduler.h"
#include "src/client/request_closure.h"

namespace curve {
namespace client {
int CopysetClient::Init(MetaCache *metaCache,
                        const IOSenderOption_t& ioSenderOpt,
                        RequestScheduler* scheduler,
                        FileMetric* fileMetric) {
    if (nullptr == metaCache || scheduler == nullptr) {
        LOG(ERROR) << "metacache or scheduler is null!";
        return -1;
    }

    metaCache_ = metaCache;
    scheduler_ = scheduler;
    fileMetric_ = fileMetric;
    senderManager_ = new(std::nothrow) RequestSenderManager();
    if (nullptr == senderManager_) {
        return -1;
    }
    iosenderopt_ = ioSenderOpt;

    inflightCntl_.SetMaxInflightNum(iosenderopt_.inflightOpt.maxInFlightRPCNum);

    LOG(INFO) << "CopysetClient init success, conf info: "
              << ", opRetryIntervalUs = "
              << iosenderopt_.failRequestOpt.opRetryIntervalUs
              << ", opMaxRetry = "
              << iosenderopt_.failRequestOpt.opMaxRetry;
    return 0;
}
bool CopysetClient::FetchLeader(LogicPoolID lpid, CopysetID cpid,
                                ChunkServerID* leaderid,
                                butil::EndPoint* leaderaddr) {
    if (-1 == metaCache_->GetLeader(lpid, cpid, leaderid,
                                    leaderaddr, false, fileMetric_)) {
        if (-1 == metaCache_->GetLeader(lpid, cpid, leaderid,
                                    leaderaddr, true, fileMetric_)) {
            LOG(WARNING) << "Get leader address form cache failed, but "
                            << "also refresh leader address failed from mds."
                            << "(<" << lpid << ", " << cpid << ">)";
            return false;
        }
    }
    return true;
}

void CopysetClient::GetInflightRPCToken() {
    inflightCntl_.GetInflightToken();
    MetricHelper::IncremInflightRPC(fileMetric_);
}

void CopysetClient::ReleaseInflightRPCToken() {
    inflightCntl_.ReleaseInflightToken();
    MetricHelper::DecremInflightRPC(fileMetric_);
}

void CopysetClient::UnInit() {
    // 等待所有RPC都回来之后才能返回
    inflightCntl_.WaitInflightAllComeBack();
}

// 因为这里的CopysetClient::ReadChunk(会在两个逻辑里调用
// 1. 从request scheduler下发的新的请求
// 2. clientclosure再重试逻辑里调用copyset client重试
// 这两种状况都会调用该接口，因为对于重试的RPC有可能需要重新push到队列中
// 非重试的RPC如果重新push到队列中会导致死锁。
int CopysetClient::ReadChunk(ChunkIDInfo idinfo,
                             uint64_t sn,
                             off_t offset,
                             size_t length,
                             uint64_t appliedindex,
                             google::protobuf::Closure *done,
                             bool needReschedule) {
    brpc::ClosureGuard doneGuard(done);

    // session过期，rpc不重试，直接push到调度队列，等待下次调度
    // 重试调用是在brpc的线程里，所以这里不会卡住重试的RPC，这样
    // 不会阻塞brpc线程，因为brpc线程是所有文件公用的。避免影响其他文件
    // 因为session续约失败可能只是网络问题，等待续约成功之后IO其实还可以
    // 正常下发，所以不能直接向上返回失败，在底层hang住，等续约成功之后继续发送
    if (sessionNotValid_ == true && exitFlag_) {
        // session过期退出时，直接返回，默认的返回值为-1
        LOG(WARNING) << " return directly for session not valid at exit!"
                    << ", copyset id = " << idinfo.cpid_
                    << ", logical pool id = " << idinfo.lpid_
                    << ", chunk id = " << idinfo.cid_
                    << ", offset = " << offset
                    << ", len = " << length;
        return 0;
    }

    RequestClosure* reqclosure = static_cast<RequestClosure*>(done);

    // 对于所有重试的RPC直接放入队列头部重新调度
    if (needReschedule) {
        doneGuard.release();
        LOG(WARNING) << "read retry rpc reEnqueue!";
        int ret = scheduler_->ReSchedule(reqclosure->GetReqCtx());
        if (ret == -1) {
            // 如果调度失败直接向上返回，默认request closure返回-1
            LOG(ERROR) << "write rpc ReSchedule error, return to user!";
            brpc::ClosureGuard doneGuard(done);
        }
        return 0;
    }

    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    while (reqclosure->GetRetriedTimes() <
          iosenderopt_.failRequestOpt.opMaxRetry) {
        reqclosure->IncremRetriedTimes();
        if (false == FetchLeader(idinfo.lpid_, idinfo.cpid_,
                                    &leaderId, &leaderAddr)) {
            bthread_usleep(iosenderopt_.failRequestOpt.opRetryIntervalUs);
            continue;
        }

        // 成功获取了leader id
        auto senderPtr = senderManager_->GetOrCreateSender(leaderId,
                                                    leaderAddr,
                                                    iosenderopt_);
        if (nullptr != senderPtr) {
            ReadChunkClosure *readDone = new ReadChunkClosure(this,
                                                     doneGuard.release());
            readDone->GetToken();
            senderPtr->ReadChunk(idinfo, sn, offset, length,
                                 appliedindex, readDone);
            /* 成功发起read，break出去，重试逻辑进入ReadChunkClosure回调 */
            break;
        } else {
            // 如果建立连接失败，再等一定时间再重试
            bthread_usleep(iosenderopt_.failRequestOpt.opRetryIntervalUs);
            continue;
        }
    }

    return 0;
}

int CopysetClient::WriteChunk(ChunkIDInfo idinfo,
                              uint64_t sn,
                              const char *buf,
                              off_t offset,
                              size_t length,
                              google::protobuf::Closure *done,
                              bool needReschedule) {
    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;
    brpc::ClosureGuard doneGuard(done);

    // session过期，rpc不重试，直接push到调度队列，等待下次调度
    // 重试调用是在brpc的线程里，所以这里不会卡住重试的RPC，这样
    // 不会阻塞brpc线程，因为brpc线程是所有文件公用的。避免影响其他文件
    // 因为session续约失败可能只是网络问题，等待续约成功之后IO其实还可以
    // 正常下发，所以不能直接向上返回失败，在底层hang住，等续约成功之后继续发送
    if (sessionNotValid_ == true && exitFlag_) {
        // session过期退出时，直接返回，默认的返回值为-1
        LOG(WARNING) << " return directly for session not valid at exit!"
                    << ", copyset id = " << idinfo.cpid_
                    << ", logical pool id = " << idinfo.lpid_
                    << ", chunk id = " << idinfo.cid_
                    << ", offset = " << offset
                    << ", len = " << length;
        return 0;
    }

    RequestClosure* reqclosure = static_cast<RequestClosure*>(done);

    // 对于所有重试的RPC直接放入队列头部重新调度
    if (needReschedule) {
        doneGuard.release();
        LOG(WARNING) << "write retry rpc reEnqueue!";
        int ret = scheduler_->ReSchedule(reqclosure->GetReqCtx());
        if (ret == -1) {
            // 如果调度失败直接向上返回，默认request closure返回-1
            LOG(ERROR) << "write rpc ReSchedule error, return to user!";
            brpc::ClosureGuard doneGuard(done);
        }
        return 0;
    }

    while (reqclosure->GetRetriedTimes() <
         iosenderopt_.failRequestOpt.opMaxRetry) {
        reqclosure->IncremRetriedTimes();
        if (false == FetchLeader(idinfo.lpid_, idinfo.cpid_,
                                    &leaderId, &leaderAddr)) {
            bthread_usleep(iosenderopt_.failRequestOpt.opRetryIntervalUs);
            continue;
        }

        auto senderPtr = senderManager_->GetOrCreateSender(leaderId,
                                                leaderAddr,
                                                iosenderopt_);
        if (nullptr != senderPtr) {
            WriteChunkClosure * writeDone = new WriteChunkClosure(this,
                                                     doneGuard.release());
            writeDone->GetToken();
            senderPtr->WriteChunk(idinfo, sn, buf, offset, length, writeDone);
            // 成功发起write，break出去，重试逻辑进 WriteChunkClosure回调
            break;
        } else {
            // 如果建立连接失败，再等一定时间再重试
            LOG(ERROR) << "create or reset sender failed (write <"
                       << idinfo.lpid_ << ", " << idinfo.cpid_ << ">)";
            bthread_usleep(iosenderopt_.failRequestOpt.
                            opRetryIntervalUs);
            continue;
        }
    }

    return 0;
}

// TODO(tongguangxun): 后面的这些快照接口也需要设置同样的FetchLeader逻辑
int CopysetClient::ReadChunkSnapshot(ChunkIDInfo idinfo,
                                     uint64_t sn,
                                     off_t offset,
                                     size_t length,
                                     Closure *done,
                                     uint64_t retriedTimes) {
    brpc::ClosureGuard doneGuard(done);

    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    for (unsigned int i = retriedTimes;
        i < iosenderopt_.failRequestOpt.opMaxRetry; ++i) {
        if (false == FetchLeader(idinfo.lpid_, idinfo.cpid_,
                                    &leaderId, &leaderAddr)) {
            bthread_usleep(iosenderopt_.failRequestOpt.opRetryIntervalUs);
            continue;
        }

        /* 成功获取了leader id */
        auto senderPtr = senderManager_->GetOrCreateSender(leaderId,
                                                    leaderAddr,
                                                    iosenderopt_);
        if (nullptr != senderPtr) {
            /* 如果建立连接成功 */
            ReadChunkSnapClosure *readDone =
                new ReadChunkSnapClosure(this, doneGuard.release());
            readDone->SetRetriedTimes(i);
            senderPtr->ReadChunkSnapshot(idinfo, sn, offset, length, readDone);
            /**
             * 成功发起 read snapshot，break出去，
             * 重试逻辑进入 ReadChunkClosure 回调
             */
            break;
        } else {
            /* 如果建立连接失败，再等一定时间再重试 */
            bthread_usleep(iosenderopt_.failRequestOpt.
                            opRetryIntervalUs);
            continue;
        }
    }

    return 0;
}

int CopysetClient::DeleteChunkSnapshotOrCorrectSn(ChunkIDInfo idinfo,
                                                  uint64_t correctedSn,
                                                  Closure *done,
                                                  uint64_t retriedTimes) {
    brpc::ClosureGuard doneGuard(done);

    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    for (unsigned int i = retriedTimes;
         i < iosenderopt_.failRequestOpt.opMaxRetry; ++i) {
        if (false == FetchLeader(idinfo.lpid_, idinfo.cpid_,
                                    &leaderId, &leaderAddr)) {
            bthread_usleep(iosenderopt_.failRequestOpt.opRetryIntervalUs);
            continue;
        }

        /* 成功获取 leader id */
        auto senderPtr = senderManager_->GetOrCreateSender(leaderId,
                                                    leaderAddr,
                                                    iosenderopt_);
        if (nullptr != senderPtr) {
            /* 如果建立连接成功 */
            DeleteChunkSnapClosure *deleteDone =
                new DeleteChunkSnapClosure(this, doneGuard.release());
            deleteDone->SetRetriedTimes(i);
            senderPtr->DeleteChunkSnapshotOrCorrectSn(idinfo,
                                                      correctedSn,
                                                      deleteDone);
            /**
             * 成功发起 delete，break出去，
             * 重试逻辑进入 DeleteChunkSnapClosure 回调
             */
            break;
        } else {
            /* 如果建立连接失败，再等一定时间再重试 */
            bthread_usleep(iosenderopt_.failRequestOpt.
                            opRetryIntervalUs);
            continue;
        }
    }

    return 0;
}

int CopysetClient::GetChunkInfo(ChunkIDInfo idinfo,
                                Closure *done,
                                uint64_t retriedTimes) {
    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;
    brpc::ClosureGuard doneGuard(done);

    for (unsigned int i = retriedTimes;
         i < iosenderopt_.failRequestOpt.opMaxRetry; ++i) {
        if (false == FetchLeader(idinfo.lpid_, idinfo.cpid_,
                                    &leaderId, &leaderAddr)) {
            bthread_usleep(iosenderopt_.failRequestOpt.opRetryIntervalUs);
            continue;
        }

        auto senderPtr = senderManager_->GetOrCreateSender(leaderId,
                                                    leaderAddr,
                                                    iosenderopt_);
        if (nullptr != senderPtr) {
            GetChunkInfoClosure *chunkInfoDone
                = new GetChunkInfoClosure(this, doneGuard.release());
            chunkInfoDone->SetRetriedTimes(i);
            senderPtr->GetChunkInfo(idinfo, chunkInfoDone);
            /* 成功发起，break出去，重试逻辑进入GetChunkInfoClosure回调 */
            break;
        } else {
            /* 如果建立连接失败，再等一定时间再重试 */
            LOG(ERROR) << "create or reset sender failed (write <"
                       << idinfo.lpid_ << ", " << idinfo.cpid_ << ">)";
            bthread_usleep(iosenderopt_.failRequestOpt.
                            opRetryIntervalUs);
            continue;
        }
    }

    return 0;
}

int CopysetClient::CreateCloneChunk(ChunkIDInfo idinfo,
                                    const std::string &location,
                                    uint64_t sn,
                                    uint64_t correntSn,
                                    uint64_t chunkSize,
                                    Closure *done,
                                    uint64_t retriedTimes) {
    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;
    brpc::ClosureGuard doneGuard(done);

    for (unsigned int i = retriedTimes;
         i < iosenderopt_.failRequestOpt.opMaxRetry; ++i) {
        if (false == FetchLeader(idinfo.lpid_, idinfo.cpid_,
                                    &leaderId, &leaderAddr)) {
            bthread_usleep(iosenderopt_.failRequestOpt.opRetryIntervalUs);
            continue;
        }

        auto senderPtr = senderManager_->GetOrCreateSender(leaderId,
                                                    leaderAddr,
                                                    iosenderopt_);
        if (nullptr != senderPtr) {
            CreateCloneChunkClosure *createCloneDone
                = new CreateCloneChunkClosure(this, doneGuard.release());
            createCloneDone->SetRetriedTimes(i);
            senderPtr->CreateCloneChunk(idinfo,
                                    createCloneDone,
                                    location,
                                    correntSn,
                                    sn,
                                    chunkSize);
            /* 成功发起，break出去，重试逻辑进入CreateCloneChunkClosure回调 */
            break;
        } else {
            /* 如果建立连接失败，再等一定时间再重试 */
            LOG(ERROR) << "create or reset sender failed (write <"
                       << idinfo.lpid_ << ", " << idinfo.cpid_ << ">)";
            bthread_usleep(iosenderopt_.failRequestOpt.
                            opRetryIntervalUs);
            continue;
        }
    }

    return 0;
}

int CopysetClient::RecoverChunk(ChunkIDInfo idinfo,
                                    uint64_t offset,
                                    uint64_t len,
                                    Closure *done,
                                    uint64_t retriedTimes) {
    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;
    brpc::ClosureGuard doneGuard(done);

    for (unsigned int i = retriedTimes;
         i < iosenderopt_.failRequestOpt.opMaxRetry; ++i) {
        if (false == FetchLeader(idinfo.lpid_, idinfo.cpid_,
                                    &leaderId, &leaderAddr)) {
            bthread_usleep(iosenderopt_.failRequestOpt.opRetryIntervalUs);
            continue;
        }

        auto senderPtr = senderManager_->GetOrCreateSender(leaderId,
                                                    leaderAddr,
                                                    iosenderopt_);
        if (nullptr != senderPtr) {
            RecoverChunkClosure *recoverChunkDone
                = new RecoverChunkClosure(this, doneGuard.release());
            recoverChunkDone->SetRetriedTimes(i);
            senderPtr->RecoverChunk(idinfo,
                                    recoverChunkDone,
                                    offset,
                                    len);
            /* 成功发起，break出去，重试逻辑进入RecoverChunkClosure回调 */
            break;
        } else {
            /* 如果建立连接失败，再等一定时间再重试 */
            LOG(ERROR) << "create or reset sender failed (write <"
                       << idinfo.lpid_ << ", " << idinfo.cpid_ << ">)";
            bthread_usleep(iosenderopt_.failRequestOpt.
                            opRetryIntervalUs);
            continue;
        }
    }

    return 0;
}
}   // namespace client
}   // namespace curve
