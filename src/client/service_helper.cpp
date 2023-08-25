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
 * File Created: Wednesday, 26th December 2018 12:28:38 pm
 * Author: tongguangxun
 */

#include "src/client/service_helper.h"

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <memory>
#include <set>
#include <utility>
#include "src/client/client_config.h"
#include "src/client/client_metric.h"
#include "src/common/curve_define.h"

namespace curve {
namespace client {

using ::curve::common::kDefaultBlockSize;

void ServiceHelper::ProtoFileInfo2Local(const curve::mds::FileInfo& finfo,
                                        FInfo_t* fi, FileEpoch_t* fEpoch) {
    if (finfo.has_owner()) {
        fi->owner = finfo.owner();
    }
    if (finfo.has_filename()) {
        fi->filename = finfo.filename();
    }
    if (finfo.has_id()) {
        fi->id = finfo.id();
    }
    if (finfo.has_parentid()) {
        fi->parentid = finfo.parentid();
    }
    if (finfo.has_filetype()) {
        fi->filetype = static_cast<FileType>(finfo.filetype());
    }
    if (finfo.has_chunksize()) {
        fi->chunksize = finfo.chunksize();
    }
    if (finfo.has_length()) {
        fi->length = finfo.length();
    }
    if (finfo.has_ctime()) {
        fi->ctime = finfo.ctime();
    }
    if (finfo.has_chunksize()) {
        fi->chunksize = finfo.chunksize();
    }
    if (finfo.has_segmentsize()) {
        fi->segmentsize = finfo.segmentsize();
    }
    if (finfo.has_seqnum()) {
        fi->seqnum = finfo.seqnum();
    }
    for (int i = 0; i < finfo.snaps_size(); i++) {
        fi->snaps.emplace_back(finfo.snaps(i));
    }
    if (finfo.has_filestatus()) {
        fi->filestatus = (FileStatus)finfo.filestatus();
    }
    if (finfo.has_stripeunit()) {
        fi->stripeUnit = finfo.stripeunit();
    }
    if (finfo.has_stripecount()) {
        fi->stripeCount = finfo.stripecount();
    }
    if (finfo.has_blocksize()) {
        fi->blocksize = finfo.blocksize();
    } else {
        // for backward compatibility
        fi->blocksize = kDefaultBlockSize;
    }
    if (finfo.has_poolset()) {
        fi->poolset = finfo.poolset();
    }

    fEpoch->fileId = finfo.id();
    if (finfo.has_epoch()) {
        fEpoch->epoch = finfo.epoch();
    } else {
        fEpoch->epoch = 0;
    }

    if ((finfo.has_filetype()) && 
        (finfo.filetype() == curve::mds::FileType::INODE_CLONE_PAGEFILE)) {
        for (int i = 0; i < finfo.clones_size(); i++) {
            CloneInfo cinfo;
            cinfo.fileId = finfo.clones(i).fileid();
            cinfo.cloneSn = finfo.clones(i).clonesn();
            fi->cloneChain.push_back(cinfo);
        }
    }
}

void ServiceHelper::ProtoCloneSourceInfo2Local(
    const curve::mds::OpenFileResponse& openFileResponse,
    CloneSourceInfo* info) {
    const curve::mds::FileInfo& fileInfo = openFileResponse.fileinfo();
    const curve::mds::CloneSourceSegment& sourceSegment =
        openFileResponse.clonesourcesegment();

    info->name = fileInfo.clonesource();
    info->length = fileInfo.clonelength();
    info->segmentSize = sourceSegment.segmentsize();
    for (int i = 0; i < sourceSegment.allocatedsegmentoffset_size(); ++i) {
        info->allocatedSegmentOffsets.insert(
            sourceSegment.allocatedsegmentoffset(i));
    }
}

class GetLeaderProxy : public std::enable_shared_from_this<GetLeaderProxy> {
    friend struct GetLeaderClosure;
 public:
    GetLeaderProxy()
        : proxyId_(getLeaderProxyId.fetch_add(1, std::memory_order_relaxed)),
          finish_(false),
          success_(false) {}

    /**
     * @brief 等待GetLeader返回结果
     * @param[out] leaderId leader的id
     * @param[out] leaderAddr leader的ip地址
     * @return 0 成功 / -1 失败
     */
    int Wait(ChunkServerID* leaderId, ChunkServerAddr* leaderAddr) {
        {
            std::unique_lock<bthread::Mutex> ulk(finishMtx_);
            while (!finish_) {
                finishCv_.wait(ulk);
            }
        }

        std::lock_guard<bthread::Mutex> lk(mtx_);
        if (success_ == false) {
            LOG(WARNING) << "GetLeader failed, logicpool id = " << logicPooldId_
                         << ", copyset id = " << copysetId_
                         << ", proxy id = " << proxyId_;
            return -1;
        }

        LOG(INFO) << "GetLeader returned, logicpool id = " << logicPooldId_
                  << ", copyset id = " << copysetId_
                  << ", proxy id = " << proxyId_ << ", leader "
                  << leader_.DebugString();

        bool has_id = leader_.has_id();
        if (has_id) {
            *leaderId = leader_.id();
        }

        bool has_address = leader_.has_address();
        if (has_address) {
            leaderAddr->Parse(leader_.address());
            return leaderAddr->IsEmpty() ? -1 : 0;
        }

        return -1;
    }

    /**
     * @brief 发起GetLeader请求
     * @param peerAddresses 除当前leader以外的peer地址
     * @param logicPoolId getleader请求的logicpool id
     * @param copysetId getleader请求的copyset id
     * @param fileMetric metric统计
     */
    void StartGetLeader(const std::unordered_set<std::string>& peerAddresses,
                        const GetLeaderRpcOption& rpcOption,
                        LogicPoolID logicPoolId, CopysetID copysetId,
                        FileMetric* fileMetric) {
        logicPooldId_ = logicPoolId;
        copysetId_ = copysetId;

        {
            std::lock_guard<bthread::Mutex> lk(mtx_);
            for (const auto& ipPort : peerAddresses) {
                std::unique_ptr<brpc::Channel> channel(new brpc::Channel());
                int ret = channel->Init(ipPort.c_str(), nullptr);
                if (ret != 0) {
                    LOG(WARNING)
                        << "GetLeader init channel to " << ipPort << " failed, "
                        << "logicpool id = " << logicPoolId
                        << "copyset id = " << copysetId;
                    continue;
                }

                GetLeaderClosure* done = new GetLeaderClosure(
                    logicPooldId_, copysetId_, shared_from_this());

                done->cntl.set_timeout_ms(rpcOption.rpcTimeoutMs);
                callIds_.emplace(done->cntl.call_id());

                channels_.emplace_back(std::move(channel));
                closures_.emplace_back(done);
            }

            if (channels_.empty()) {
                std::lock_guard<bthread::Mutex> ulk(finishMtx_);
                finish_ = true;
                success_ = false;
                finishCv_.notify_one();
                return;
            }
        }

        for (int i = 0; i < channels_.size(); ++i) {
            curve::chunkserver::CliService2_Stub stub(channels_[i].get());
            curve::chunkserver::GetLeaderRequest2 request;
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);

            MetricHelper::IncremGetLeaderRetryTime(fileMetric);
            stub.GetLeader(&(closures_[i]->cntl), &request,
                           &(closures_[i]->response), closures_[i]);
        }
    }

    /**
     * @brief 处理异步请求结果
     * @param callId rpc请求id
     * @param success rpc请求是否成功
     * @param peer rpc请求返回的leader信息
     */
    void HandleResponse(brpc::CallId callId, bool success,
                        const curve::common::Peer& peer) {
        std::lock_guard<bthread::Mutex> lk(mtx_);

        if (finish_) {
            return;
        }

        if (success) {
            for (auto id : callIds_) {
                if (id == callId) {
                    continue;
                }

                // cancel以后,后续的rpc请求回调仍然会执行,但是会标记为失败
                brpc::StartCancel(id);
            }

            callIds_.clear();
            leader_ = peer;

            std::lock_guard<bthread::Mutex> ulk(finishMtx_);
            finish_ = true;
            success_ = true;
            finishCv_.notify_one();
        } else {
            // 删除当前call id
            callIds_.erase(callId);

            // 如果为空，说明是最后一个rpc返回，需要标记请求失败，并向上返回
            if (callIds_.empty()) {
                std::lock_guard<bthread::Mutex> ulk(finishMtx_);
                finish_ = true;
                success_ = false;
                finishCv_.notify_one();
            }
        }
    }

 private:
    uint64_t proxyId_;

    // 是否完成请求
    //   1. 其中一个请求成功
    //   2. 最后一个请求返回
    // 都会标记为true
    bool finish_;
    bthread::ConditionVariable finishCv_;
    bthread::Mutex finishMtx_;

    // 记录cntl id
    std::set<brpc::CallId> callIds_;

    // 请求是否成功
    bool success_;

    // leader信息
    curve::common::Peer leader_;

    // 保护callIds_/success_，避免异步rpc回调同时操作
    bthread::Mutex mtx_;

    LogicPoolID logicPooldId_;
    CopysetID copysetId_;

    std::vector<std::unique_ptr<brpc::Channel>> channels_;
    std::vector<GetLeaderClosure*> closures_;

    static std::atomic<uint64_t> getLeaderProxyId;
};

std::atomic<uint64_t> GetLeaderProxy::getLeaderProxyId{0};

void GetLeaderClosure::Run() {
    std::unique_ptr<GetLeaderClosure> selfGuard(this);
    if (proxy == nullptr) {
        LOG(ERROR) << "proxy invalid";
        return;
    }

    bool success = false;
    if (cntl.Failed()) {
        success = false;
        LOG_IF(WARNING, cntl.ErrorCode() != ECANCELED)
            << "GetLeader failed from " << cntl.remote_side()
            << ", logicpool id = " << logicPoolId
            << ", copyset id = " << copysetId
            << ", proxy id = " << proxy->proxyId_
            << ", error = " << cntl.ErrorText();
    } else {
        success = true;
        LOG(INFO) << "GetLeader returned from " << cntl.remote_side()
                    << ", logicpool id = " << logicPoolId
                    << ", copyset id = " << copysetId
                    << ", proxy id = " << proxy->proxyId_
                    << ", leader = " << response.DebugString();
    }
    proxy->HandleResponse(cntl.call_id(), success, response.leader());
}

int ServiceHelper::GetLeader(const GetLeaderInfo& getLeaderInfo,
                             ChunkServerAddr* leaderAddr,
                             ChunkServerID* leaderId,
                             FileMetric* fileMetric) {
    const auto& peerInfo = getLeaderInfo.copysetPeerInfo;

    int16_t index = -1;
    leaderAddr->Reset();

    std::unordered_set<std::string> chunkserverIpPorts;
    for (auto iter = peerInfo.begin(); iter != peerInfo.end(); ++iter) {
        ++index;
        if (index == getLeaderInfo.currentLeaderIndex) {
            LOG(INFO) << "refresh leader skip current leader address: "
                      << iter->externalAddr.ToString().c_str()
                      << ", logicpoolid = " << getLeaderInfo.logicPoolId
                      << ", copysetid = " << getLeaderInfo.copysetId;
            continue;
        }

        chunkserverIpPorts.emplace(
            butil::endpoint2str(iter->externalAddr.addr_).c_str());
    }

    std::shared_ptr<GetLeaderProxy> proxy(std::make_shared<GetLeaderProxy>());
    proxy->StartGetLeader(chunkserverIpPorts, getLeaderInfo.rpcOption,
                          getLeaderInfo.logicPoolId, getLeaderInfo.copysetId,
                          fileMetric);
    return proxy->Wait(leaderId, leaderAddr);
}

bool ServiceHelper::GetUserInfoFromFilename(const std::string& filename,
                                            std::string* realfilename,
                                            std::string* user) {
    auto user_end = filename.find_last_of("_");
    auto user_begin = filename.find_last_of("_", user_end - 1);

    if (user_end == filename.npos || user_begin == filename.npos) {
        LOG(ERROR) << "get user info failed!";
        return false;
    }

    *realfilename = filename.substr(0, user_begin);
    *user = filename.substr(user_begin + 1, user_end - user_begin - 1);

    return true;
}

bool ServiceHelper::GetSnapSeqFromFilename(const std::string& filename, 
                                           uint64_t& sn,
                                           std::string* realfilename) {
    auto snapPos = filename.find_last_of("@");
    if (snapPos == std::string::npos || snapPos == filename.length() -1 ) {
        return false;
    }
    if (filename.find_first_not_of("0123456789", snapPos + 1) != std::string::npos ) {
        LOG(ERROR) << "filename " << filename << " contains invalid seqnum.";
        return false;
    }

    std::string snapStr = filename.substr(snapPos + 1);
    sn = std::stoul(snapStr);
    *realfilename = filename.substr(0, snapPos);
    if (sn == 0)
        return false;
    return true;
}

int ServiceHelper::CheckChunkServerHealth(
    const butil::EndPoint& endPoint, int32_t requestTimeoutMs) {
    brpc::Controller cntl;
    brpc::Channel httpChannel;
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_HTTP;

    std::string ipPort = butil::endpoint2str(endPoint).c_str();
    int res = httpChannel.Init(ipPort.c_str(), &options);
    if (res != 0) {
        LOG(WARNING) << "init http channel failed, address = " << ipPort;
        return -1;
    }

    // 访问 ip:port/health
    cntl.http_request().uri() = ipPort + "/health";
    cntl.set_timeout_ms(requestTimeoutMs);
    httpChannel.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);

    if (cntl.Failed()) {
        LOG(WARNING) << "CheckChunkServerHealth failed, " << cntl.ErrorText()
            << ", url = " << cntl.http_request().uri();
        return -1;
    } else {
        LOG(INFO) << "CheckChunkServerHealth success, "
            << cntl.response_attachment()
            << ", url = " << cntl.http_request().uri();
        return 0;
    }
}

}   // namespace client
}   // namespace curve
