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

namespace curve {
namespace client {

int CopysetClient::Init(MetaCache *metaCache,
    const IOSenderOption& ioSenderOpt, RequestScheduler* scheduler,
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
                 "chunkserverOPRetryIntervalUS = "
              << iosenderopt_.failRequestOpt.chunkserverOPRetryIntervalUS
              << ", chunkserverOPMaxRetry = "
              << iosenderopt_.failRequestOpt.chunkserverOPMaxRetry
              << ", chunkserverMaxRPCTimeoutMS = "
              << iosenderopt_.failRequestOpt.chunkserverMaxRPCTimeoutMS;
    return 0;
}
bool CopysetClient::FetchLeader(LogicPoolID lpid, CopysetID cpid,
    ChunkServerID* leaderid, butil::EndPoint* leaderaddr) {
    // 1. First, pull the leader information from the current metacache
    if (0 == metaCache_->GetLeader(lpid, cpid, leaderid,
        leaderaddr, false, fileMetric_)) {
        return true;
    }

    // 2. If the pull of leader information in the metacache fails, send an RPC request to obtain new leader information
    if (-1 == metaCache_->GetLeader(lpid, cpid, leaderid,
        leaderaddr, true, fileMetric_)) {
        LOG(WARNING) << "Get leader address form cache failed, but "
            << "also refresh leader address failed from mds."
            << "(<" << lpid << ", " << cpid << ">)";
        return false;
    }

    return true;
}

// Because the CopysetClient::ReadChunk (will be called in two logics) here
// 1. New requests issued from the request scheduler
// 2. Calling copyset client to retry in the clientclosure retry logic
// Both of these situations will call the interface, as retrying RPCs may require re pushing to the queue
// If non retrying RPC is pushed back into the queue, it will cause a deadlock.
int CopysetClient::ReadChunk(const ChunkIDInfo& idinfo, uint64_t sn,
                             off_t offset, size_t length,
                             const RequestSourceInfo& sourceInfo,
                             google::protobuf::Closure* done) {
    RequestClosure* reqclosure = static_cast<RequestClosure*>(done);
    brpc::ClosureGuard doneGuard(done);

    // There are two scenarios for retrying when a session expires:
    // 1. During the normal retry process, if the file is not in a closed state, RPC will directly re push to the scheduler queue header
    //    The retry call is in the brpc thread, so there will be no blocking of the retry RPC here
    //    Will not block the brpc thread as it is common to all files. Avoid affecting other files
    //    Because the session renewal failure may only be a network issue, IO is actually still possible after the renewal is successful
    //    Normal distribution, so failure cannot be directly returned upwards. Hang on at the bottom and continue sending after the renewal is successful
    // 2. exitFlag_=true during file closing, retrying rpc will directly return to the user through closure
    //    After the return call, doneguard will call the run of the closure, releasing the inflight rpc count,
    //    Then the closure is returned to the user upwards.
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
            // After the session expires, it needs to be re pushed to the queue
            LOG(WARNING) << "session not valid, read rpc ReSchedule!";
            doneGuard.release();
            reqclosure->ReleaseInflightRPCToken();
            scheduler_->ReSchedule(reqclosure->GetReqCtx());
            return 0;
        }
    }

    auto task = [&](Closure* done, std::shared_ptr<RequestSender> senderPtr) {
        ReadChunkClosure *readDone = new ReadChunkClosure(this, done);
        senderPtr->ReadChunk(idinfo, sn, offset,
                             length, sourceInfo, readDone);
    };

    return DoRPCTask(idinfo, task, doneGuard.release());
}

int CopysetClient::WriteChunk(const ChunkIDInfo& idinfo,
                              uint64_t fileId,
                              uint64_t epoch,
                              uint64_t sn,
                              const butil::IOBuf& data,
                              off_t offset, size_t length,
                              const RequestSourceInfo& sourceInfo,
                              google::protobuf::Closure* done) {
    std::shared_ptr<RequestSender> senderPtr = nullptr;
    butil::EndPoint leaderAddr;

    RequestClosure* reqclosure = static_cast<RequestClosure*>(done);

    brpc::ClosureGuard doneGuard(done);

    // There are two scenarios for retrying when a session expires:
    // 1. During the normal retry process, if the file is not in a closed state, RPC will directly re push to the scheduler queue header
    //    The retry call is in the brpc thread, so there will be no blocking of the retry RPC here
    //    Will not block the brpc thread as it is common to all files. Avoid affecting other files
    //    Because the session renewal failure may only be a network issue, IO is actually still possible after the renewal is successful
    //    Normal distribution, so failure cannot be directly returned upwards. Hang on at the bottom and continue sending after the renewal is successful
    // 2. exitFlag_=true during file closing, retrying rpc will directly return to the user through closure
    //    After the return call, doneguard will call the run of the closure, releasing the inflight rpc count,
    //    Then the closure is returned to the user upwards.
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
        senderPtr->WriteChunk(idinfo, fileId, epoch, sn,
                              data, offset, length, sourceInfo,
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
