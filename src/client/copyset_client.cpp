/*
 * Project: curve
 * Created Date: 18-9-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/client/copyset_client.h"

#include <glog/logging.h>
#include <unistd.h>

#include <utility>

#include "src/client/request_sender.h"
#include "src/client/metacache.h"
#include "src/client/client_config.h"

namespace curve {
namespace client {

int CopysetClient::Init(MetaCache *metaCache,
                        IOSenderOption_t ioSenderOpt) {
    if (nullptr == metaCache) {
        return -1;
    }

    metaCache_ = metaCache;
    senderManager_ = new(std::nothrow) RequestSenderManager();
    if (nullptr == senderManager_) {
        return -1;
    }
    iosenderopt_ = ioSenderOpt;
    return 0;
}

int CopysetClient::ReadChunk(ChunkIDInfo idinfo,
                             uint64_t sn,
                             off_t offset,
                             size_t length,
                             uint64_t appliedindex,
                             google::protobuf::Closure *done,
                             uint16_t retriedTimes) {
    brpc::ClosureGuard doneGuard(done);

    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    for (unsigned int i = retriedTimes;
        i < iosenderopt_.failRequestOpt.opMaxRetry; ++i) {
        /* cache中找 */
        if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                        &leaderId, &leaderAddr, false)) {
            /* 没找到刷新cache */
            if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                            &leaderId, &leaderAddr, true)) {
                LOG(WARNING) << "Get leader address form cache failed, but "
                             << "also refresh leader address failed from mds."
                             << "(write <" << idinfo.lpid_
                             << ", " << idinfo.cpid_ << ">)";
                /* 刷新cache失败，再等一定时间再重试 */
                bthread_usleep(iosenderopt_.failRequestOpt.
                                opRetryIntervalUs);
                continue;
            }
        }

        /* 成功获取了leader id */
        auto senderPtr = senderManager_->GetOrCreateSender(leaderId,
                                                    leaderAddr,
                                                    iosenderopt_);
        if (nullptr != senderPtr) {
            /* 如果建立连接成功 */
            ReadChunkClosure *readDone =
                new ReadChunkClosure(this, doneGuard.release());
            readDone->SetRetriedTimes(i);
            senderPtr->ReadChunk(idinfo, sn, offset, length,
                                 appliedindex, readDone);
            /* 成功发起read，break出去，重试逻辑进入ReadChunkClosure回调 */
            break;
        } else {
            /* 如果建立连接失败，再等一定时间再重试 */
            usleep(iosenderopt_.failRequestOpt.
                    opRetryIntervalUs);
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
                              uint16_t retriedTimes) {
    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;
    brpc::ClosureGuard doneGuard(done);

    for (unsigned int i = retriedTimes;
        i < iosenderopt_.failRequestOpt.opMaxRetry; ++i) {
        /* cache中找 */
        if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                        &leaderId, &leaderAddr, false)) {
            /* 没找到刷新cache */
            if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                            &leaderId, &leaderAddr, true)) {
                LOG(WARNING) << "Get leader address form cache failed, but "
                        << "also refresh leader address failed from mds."
                        << "(write <" << idinfo.lpid_ << ", " << idinfo.cpid_
                        << ">)";
                /* 刷新cache失败，再等一定时间再重试 */
                bthread_usleep(iosenderopt_.failRequestOpt.
                                opRetryIntervalUs);
                continue;
            }
        }

        auto senderPtr = senderManager_->GetOrCreateSender(leaderId,
                                                leaderAddr,
                                                iosenderopt_);
        if (nullptr != senderPtr) {
            WriteChunkClosure *writeDone
                = new WriteChunkClosure(this, doneGuard.release());
            writeDone->SetRetriedTimes(i);
            senderPtr->WriteChunk(idinfo, sn, buf, offset, length, writeDone);
            /* 成功发起write，break出去，重试逻辑进 WriteChunkClosure回调 */
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

int CopysetClient::ReadChunkSnapshot(ChunkIDInfo idinfo,
                                     uint64_t sn,
                                     off_t offset,
                                     size_t length,
                                     Closure *done,
                                     uint16_t retriedTimes) {
    brpc::ClosureGuard doneGuard(done);

    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    for (unsigned int i = retriedTimes;
        i < iosenderopt_.failRequestOpt.opMaxRetry; ++i) {
        /* cache中找 */
        if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                        &leaderId, &leaderAddr, false)) {
            /* 没找到刷新cache*/
            if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                            &leaderId, &leaderAddr, true)) {
                LOG(WARNING) << "Get leader address form cache failed, but "
                             << "also refresh leader address failed from mds."
                             << "(write <" << idinfo.lpid_
                             << ", " << idinfo.cpid_ << ">)";
                /* 刷新cache失败，再等一定时间再重试 */
                bthread_usleep(iosenderopt_.failRequestOpt.
                                opRetryIntervalUs);
                continue;
            }
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
                                                  uint16_t retriedTimes) {
    brpc::ClosureGuard doneGuard(done);

    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    for (unsigned int i = retriedTimes;
         i < iosenderopt_.failRequestOpt.opMaxRetry; ++i) {
        /* cache中找 */
        if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                        &leaderId, &leaderAddr, false)) {
            /* 没找到刷新cache */
            if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                            &leaderId, &leaderAddr, true)) {
                LOG(WARNING) << "Get leader address form cache failed, but "
                             << "also refresh leader address failed from mds."
                             << "(write <" << idinfo.lpid_
                             << ", " << idinfo.cpid_ << ">)";
                /* 刷新cache失败，再等一定时间再重试 */
                bthread_usleep(iosenderopt_.failRequestOpt.
                                opRetryIntervalUs);
                continue;
            }
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
                                uint16_t retriedTimes) {
    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;
    brpc::ClosureGuard doneGuard(done);

    for (unsigned int i = retriedTimes;
         i < iosenderopt_.failRequestOpt.opMaxRetry; ++i) {
        /* cache中找 */
        if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                        &leaderId, &leaderAddr, false)) {
            /* 没找到刷新cache */
            if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                            &leaderId, &leaderAddr, true)) {
                LOG(WARNING) << "Get leader address form cache failed, but "
                             << "also refresh leader address failed from mds."
                             << "(write <" << idinfo.lpid_
                             << ", " << idinfo.cpid_ << ">)";
                /* 刷新cache失败，再等一定时间再重试 */
                bthread_usleep(iosenderopt_.failRequestOpt.
                                opRetryIntervalUs);
                continue;
            }
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
                                    uint16_t retriedTimes) {
    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;
    brpc::ClosureGuard doneGuard(done);

    for (unsigned int i = retriedTimes;
         i < iosenderopt_.failRequestOpt.opMaxRetry; ++i) {
        /* cache中找 */
        if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                        &leaderId, &leaderAddr, false)) {
            /* 没找到刷新cache */
            if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                            &leaderId, &leaderAddr, true)) {
                LOG(WARNING) << "Get leader address form cache failed, but "
                             << "also refresh leader address failed from mds."
                             << "(write <" << idinfo.lpid_
                             << ", " << idinfo.cpid_ << ">)";
                /* 刷新cache失败，再等一定时间再重试 */
                bthread_usleep(iosenderopt_.failRequestOpt.
                                opRetryIntervalUs);
                continue;
            }
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
                                    uint16_t retriedTimes) {
    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;
    brpc::ClosureGuard doneGuard(done);

    for (unsigned int i = retriedTimes;
         i < iosenderopt_.failRequestOpt.opMaxRetry; ++i) {
        /* cache中找 */
        if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                        &leaderId, &leaderAddr, false)) {
            /* 没找到刷新cache */
            if (-1 == metaCache_->GetLeader(idinfo.lpid_, idinfo.cpid_,
                                            &leaderId, &leaderAddr, true)) {
                LOG(WARNING) << "Get leader address form cache failed, but "
                             << "also refresh leader address failed from mds."
                             << "(write <" << idinfo.lpid_
                             << ", " << idinfo.cpid_ << ">)";
                /* 刷新cache失败，再等一定时间再重试 */
                bthread_usleep(iosenderopt_.failRequestOpt.
                                opRetryIntervalUs);
                continue;
            }
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
