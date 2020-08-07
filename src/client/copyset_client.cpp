/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 18-9-25
 * Author: wudemiao
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

using google::protobuf::Closure;
namespace curve {
namespace client {
int CopysetClient::Init(MetaCache *metaCache,
    const IOSenderOption_t& ioSenderOpt, RequestScheduler* scheduler,
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

    LOG(INFO) << "CopysetClient init success, conf info: "
              << ", chunkserverOPRetryIntervalUS = "
              << iosenderopt_.failRequestOpt.chunkserverOPRetryIntervalUS
              << ", chunkserverOPMaxRetry = "
              << iosenderopt_.failRequestOpt.chunkserverOPMaxRetry
              << ", chunkserverMaxRPCTimeoutMS = "
              << iosenderopt_.failRequestOpt.chunkserverMaxRPCTimeoutMS;
    return 0;
}
bool CopysetClient::FetchLeader(LogicPoolID lpid, CopysetID cpid,
    ChunkServerID* leaderid, butil::EndPoint* leaderaddr) {
    // 1. 先去当前metacache中拉取leader信息
    if (0 == metaCache_->GetLeader(lpid, cpid, leaderid,
        leaderaddr, false, fileMetric_)) {
        return true;
    }

    // 2. 如果metacache中leader信息拉取失败，就发送RPC请求获取新leader信息
    if (-1 == metaCache_->GetLeader(lpid, cpid, leaderid,
        leaderaddr, true, fileMetric_)) {
        LOG(WARNING) << "Get leader address form cache failed, but "
            << "also refresh leader address failed from mds."
            << "(<" << lpid << ", " << cpid << ">)";
        return false;
    }

    return true;
}

// 因为这里的CopysetClient::ReadChunk(会在两个逻辑里调用
// 1. 从request scheduler下发的新的请求
// 2. clientclosure再重试逻辑里调用copyset client重试
// 这两种状况都会调用该接口，因为对于重试的RPC有可能需要重新push到队列中
// 非重试的RPC如果重新push到队列中会导致死锁。
int CopysetClient::ReadChunk(const ChunkIDInfo& idinfo, uint64_t sn,
                             off_t offset, size_t length, uint64_t appliedindex,
                             const RequestSourceInfo& sourceInfo,
                             google::protobuf::Closure* done) {
    RequestClosure* reqclosure = static_cast<RequestClosure*>(done);
    brpc::ClosureGuard doneGuard(done);

    // session过期情况下重试有两种场景：
    // 1. 正常重试过程，非文件关闭状态，这时候RPC直接重新push到scheduler队列头部
    //     重试调用是在brpc的线程里，所以这里不会卡住重试的RPC，这样
    //     不会阻塞brpc线程，因为brpc线程是所有文件公用的。避免影响其他文件
    //     因为session续约失败可能只是网络问题，等待续约成功之后IO其实还可以
    //     正常下发，所以不能直接向上返回失败，在底层hang住，等续约成功之后继续发送
    // 2. 在关闭文件过程中exitFlag_=true，重试rpc会直接向上通过closure返回给用户
    //     return调用之后doneguard会调用closure的run，会释放inflight rpc计数，
    //     然后closure向上返回给用户。
    if (sessionNotValid_ == true) {
        if (exitFlag_) {
            LOG(WARNING) << " return directly for session not valid at exit!"
                        << ", copyset id = " << idinfo.cpid_
                        << ", logical pool id = " << idinfo.lpid_
                        << ", chunk id = " << idinfo.cid_
                        << ", offset = " << offset
                        << ", len = " << length;
            return 0;
        } else {
            // session过期之后需要重新push到队列
            LOG(WARNING) << "session not valid, read rpc ReSchedule!";
            doneGuard.release();
            reqclosure->ReleaseInflightRPCToken();
            scheduler_->ReSchedule(reqclosure->GetReqCtx());
            return 0;
        }
    }

    auto task = [&](Closure* done, std::shared_ptr<RequestSender> senderPtr) {
        ReadChunkClosure *readDone = new ReadChunkClosure(this, done);
        senderPtr->ReadChunk(idinfo, sn, offset, length,
                             appliedindex, sourceInfo, readDone);
    };

    return DoRPCTask(idinfo, task, doneGuard.release());
}

int CopysetClient::WriteChunk(const ChunkIDInfo& idinfo, uint64_t sn,
                              const butil::IOBuf& data,
                              off_t offset, size_t length,
                              const RequestSourceInfo& sourceInfo,
                              google::protobuf::Closure* done) {
    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    RequestClosure* reqclosure = static_cast<RequestClosure*>(done);

    brpc::ClosureGuard doneGuard(done);

    // session过期情况下重试有两种场景：
    // 1. 正常重试过程，非文件关闭状态，这时候RPC直接重新push到scheduler队列头部
    //     重试调用是在brpc的线程里，所以这里不会卡住重试的RPC，这样
    //     不会阻塞brpc线程，因为brpc线程是所有文件公用的。避免影响其他文件
    //     因为session续约失败可能只是网络问题，等待续约成功之后IO其实还可以
    //     正常下发，所以不能直接向上返回失败，在底层hang住，等续约成功之后继续发送
    // 2. 在关闭文件过程中exitFlag_=true，重试rpc会直接向上通过closure返回给用户
    //     return调用之后doneguard会调用closure的run，会释放inflight rpc计数，
    //     然后closure向上返回给用户。
    if (sessionNotValid_ == true) {
        if (exitFlag_) {
            LOG(WARNING) << " return directly for session not valid at exit!"
                        << ", copyset id = " << idinfo.cpid_
                        << ", logical pool id = " << idinfo.lpid_
                        << ", chunk id = " << idinfo.cid_
                        << ", offset = " << offset
                        << ", len = " << length;
            return 0;
        } else {
            LOG(WARNING) << "session not valid, write rpc ReSchedule!";
            doneGuard.release();
            reqclosure->ReleaseInflightRPCToken();
            scheduler_->ReSchedule(reqclosure->GetReqCtx());
            return 0;
        }
    }

    auto task = [&](Closure* done, std::shared_ptr<RequestSender> senderPtr) {
        WriteChunkClosure* writeDone = new WriteChunkClosure(this, done);
        senderPtr->WriteChunk(idinfo, sn, data, offset, length, sourceInfo,
                              writeDone);
    };

    return DoRPCTask(idinfo, task, doneGuard.release());
}

int CopysetClient::ReadChunkSnapshot(const ChunkIDInfo& idinfo,
    uint64_t sn, off_t offset, size_t length, Closure *done) {

    auto task = [&](Closure* done, std::shared_ptr<RequestSender> senderPtr) {
        ReadChunkSnapClosure *readDone = new ReadChunkSnapClosure(this, done);
        senderPtr->ReadChunkSnapshot(idinfo, sn, offset, length, readDone);
    };

    return DoRPCTask(idinfo, task, done);
}

int CopysetClient::DeleteChunkSnapshotOrCorrectSn(const ChunkIDInfo& idinfo,
    uint64_t correctedSn, Closure *done) {

    auto task = [&](Closure* done, std::shared_ptr<RequestSender> senderPtr) {
        DeleteChunkSnapClosure *deleteDone = new DeleteChunkSnapClosure(
                                             this, done);
        senderPtr->DeleteChunkSnapshotOrCorrectSn(idinfo,
                                              correctedSn, deleteDone);
    };

    return DoRPCTask(idinfo, task, done);
}

int CopysetClient::GetChunkInfo(const ChunkIDInfo& idinfo, Closure *done) {
    auto task = [&](Closure* done, std::shared_ptr<RequestSender> senderPtr) {
        GetChunkInfoClosure *chunkInfoDone = new GetChunkInfoClosure(this, done);   // NOLINT
        senderPtr->GetChunkInfo(idinfo, chunkInfoDone);
    };

    return DoRPCTask(idinfo, task, done);
}

int CopysetClient::CreateCloneChunk(const ChunkIDInfo& idinfo,
                                      const std::string& location, uint64_t sn,
                                      uint64_t correntSn, uint64_t chunkSize,
                                      Closure* done) {
    auto task = [&](Closure* done, std::shared_ptr<RequestSender> senderPtr) {
        CreateCloneChunkClosure* createCloneDone =
            new CreateCloneChunkClosure(this, done);
        senderPtr->CreateCloneChunk(idinfo, createCloneDone, location,
                                    correntSn, sn, chunkSize);
    };

    return DoRPCTask(idinfo, task, done);
}

int CopysetClient::RecoverChunk(const ChunkIDInfo& idinfo,
                                 uint64_t offset,
                                uint64_t len, Closure* done) {
    auto task = [&](Closure* done, std::shared_ptr<RequestSender> senderPtr) {
        RecoverChunkClosure* recoverChunkDone =
            new RecoverChunkClosure(this, done);
        senderPtr->RecoverChunk(idinfo, recoverChunkDone, offset,
                                len);
    };

    return DoRPCTask(idinfo, task, done);
}

int CopysetClient::DoRPCTask(const ChunkIDInfo& idinfo,
    std::function<void(Closure* done,
    std::shared_ptr<RequestSender> senderptr)> task, Closure *done) {
    RequestClosure* reqclosure = static_cast<RequestClosure*>(done);

    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;
    brpc::ClosureGuard doneGuard(done);

    while (reqclosure->GetRetriedTimes() <
        iosenderopt_.failRequestOpt.chunkserverOPMaxRetry) {
        reqclosure->IncremRetriedTimes();
        if (false == FetchLeader(idinfo.lpid_, idinfo.cpid_,
            &leaderId, &leaderAddr)) {
            bthread_usleep(
            iosenderopt_.failRequestOpt.chunkserverOPRetryIntervalUS);
            continue;
        }

        auto senderPtr = senderManager_->GetOrCreateSender(leaderId,
                                        leaderAddr, iosenderopt_);
        if (nullptr != senderPtr) {
            task(doneGuard.release(), senderPtr);
            break;
        } else {
            LOG(WARNING) << "create or reset sender failed, "
                << ", leaderId = " << leaderId;
            bthread_usleep(
            iosenderopt_.failRequestOpt.chunkserverOPRetryIntervalUS);
            continue;
        }
    }

    return 0;
}
}   // namespace client
}   // namespace curve
