/*
 * Project: curve
 * File Created: Wednesday, 26th December 2018 12:28:38 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <ostream>

#include "src/client/client_config.h"
#include "src/client/service_helper.h"
#include "src/client/client_metric.h"

namespace curve {
namespace client {

void ServiceHelper::ProtoFileInfo2Local(const curve::mds::FileInfo* finfo,
                                        FInfo_t* fi) {
    if (finfo->has_owner()) {
        fi->owner = finfo->owner();
    }
    if (finfo->has_filename()) {
        fi->filename = finfo->filename();
    }
    if (finfo->has_id()) {
        fi->id = finfo->id();
    }
    if (finfo->has_parentid()) {
        fi->parentid = finfo->parentid();
    }
    if (finfo->has_filetype()) {
        fi->filetype = static_cast<FileType>(finfo->filetype());
    }
    if (finfo->has_chunksize()) {
        fi->chunksize = finfo->chunksize();
    }
    if (finfo->has_length()) {
        fi->length = finfo->length();
    }
    if (finfo->has_ctime()) {
        fi->ctime = finfo->ctime();
    }
    if (finfo->has_chunksize()) {
        fi->chunksize = finfo->chunksize();
    }
    if (finfo->has_segmentsize()) {
        fi->segmentsize = finfo->segmentsize();
    }
    if (finfo->has_seqnum()) {
        fi->seqnum = finfo->seqnum();
    }
    if (finfo->has_filestatus()) {
        fi->filestatus = (FileStatus)finfo->filestatus();
    }
}

std::string ServiceHelper::BuildChannelUrl(
    const std::unordered_set<std::string>& chunkserverIpPorts) {
    std::string urls("list://");
    for (const auto& addr : chunkserverIpPorts) {
        urls.append(addr).append(",");
    }

    return urls;
}

int ServiceHelper::GetLeaderInternal(
    const GetLeaderInfo& getLeaderInfo,
    const std::unordered_set<std::string>& chunkserverIpPorts,
    FileMetric* fileMetric,
    ChunkServerAddr* leaderAddr,
    ChunkServerID* leaderId,
    int* cntlErrCode,
    std::string* cntlFailedAddr) {
    const std::string& urls = BuildChannelUrl(chunkserverIpPorts);
    LOG(INFO) << "Send GetLeader request to " << urls
        << " logicpool id = " << getLeaderInfo.logicPoolId
        << ", copyset id = " << getLeaderInfo.copysetId;

    brpc::Channel channel;
    brpc::ChannelOptions opts;
    opts.backup_request_ms = getLeaderInfo.rpcOption.backupRequestMs;

    int ret = channel.Init(urls.c_str(),
                           getLeaderInfo.rpcOption.backupRequestLbName.c_str(),
                           &opts);
    if (ret != 0) {
        LOG(ERROR) << "Fail to init channel to " << urls;
        return -1;
    }

    curve::chunkserver::CliService2_Stub stub(&channel);
    curve::chunkserver::GetLeaderRequest2 request;
    curve::chunkserver::GetLeaderResponse2 response;

    brpc::Controller cntl;
    cntl.set_timeout_ms(getLeaderInfo.rpcOption.rpcTimeoutMs);

    request.set_logicpoolid(getLeaderInfo.logicPoolId);
    request.set_copysetid(getLeaderInfo.copysetId);

    stub.GetLeader(&cntl, &request, &response, NULL);
    MetricHelper::IncremGetLeaderRetryTime(fileMetric);

    if (cntl.Failed()) {
        LOG(WARNING) << "GetLeader failed, "
                     << cntl.ErrorText()
                     << ", chunkserver addr = " << cntl.remote_side()
                     << ", logicpool id = " << getLeaderInfo.logicPoolId
                     << ", copyset id = " << getLeaderInfo.copysetId;

        *cntlErrCode = cntl.ErrorCode();
        *cntlFailedAddr = butil::endpoint2str(cntl.remote_side()).c_str();
        return -1;
    }

    LOG(INFO) << "GetLeader returned from " << cntl.remote_side()
              << ", logicpool id = " << getLeaderInfo.logicPoolId
              << ", copyset id = " << getLeaderInfo.copysetId
              << ", response = " << response.DebugString();

    bool has_id = response.leader().has_id();
    if (has_id) {
        *leaderId = response.leader().id();
    }

    bool has_address = response.leader().has_address();
    if (has_address) {
        leaderAddr->Parse(response.leader().address());
        return leaderAddr->IsEmpty() ? -1 : 0;
    }

    return -1;
}

int ServiceHelper::GetLeader(const GetLeaderInfo& getLeaderInfo,
                             ChunkServerAddr* leaderAddr,
                             ChunkServerID* leaderId,
                             FileMetric_t* fileMetric) {
    const auto& peerInfo = getLeaderInfo.copysetPeerInfo;
    if (peerInfo.empty()) {
        LOG(ERROR) << "Empty group configuration"
                   << ", logicpool id = " << getLeaderInfo.logicPoolId
                   << ", copyset id = " << getLeaderInfo.copysetId;
        return -1;
    }

    int16_t index = -1;
    leaderAddr->Reset();

    std::unordered_set<std::string> chunkserverIpPorts;
    for (auto iter = peerInfo.begin(); iter != peerInfo.end(); ++iter) {
        ++index;
        if (index == getLeaderInfo.currentLeaderIndex) {
            LOG(INFO) << "refresh leader skip current leader address: "
                      << iter->csaddr_.ToString().c_str()
                      << ", logicpoolid = " << getLeaderInfo.logicPoolId
                      << ", copysetid = " << getLeaderInfo.copysetId;
            continue;
        }

        chunkserverIpPorts.emplace(
            butil::endpoint2str(iter->csaddr_.addr_).c_str());
    }

    int ret = 0;
    int cntlErrCode = 0;
    std::string cntlFailedAddr;

    while (!chunkserverIpPorts.empty()) {
        ret = GetLeaderInternal(getLeaderInfo,
                                chunkserverIpPorts,
                                fileMetric,
                                leaderAddr,
                                leaderId,
                                &cntlErrCode,
                                &cntlFailedAddr);
        if (ret == 0) {
            return 0;
        }

        // 针对错误码，删除failed节点后进行重试
        // 假如有A B C三个节点，C是当前leader，GetLeader请求会发往A B两个节点上
        // 如果A节点比较繁忙, B所在的进程退出
        // 此时，发往A节点的返回时间超过backupRequestMs
        // 会再次发送一个到B节点的请求，由于进程退出，会很快返回，导致这次GetLeader失败
        // 由于复制组没有变化，下次请求还是出现这种情况，导致GetLeader一直失败
        // 所以需要删除B节点后进行重试
        // 1. ENOENT: chunkserver端没有对应的copyset
        // 2. EAGAIN: chunkserver端有对应的copyset，但是copyset没有leader
        // 3. EHOSTDOWN: 正常发送RPC情况下，对端进程挂掉
        // 4. ECONNREFUSED: 访问存在IP地址，但无人监听
        // 5. ECONNRESET: 对端连接已关闭
        // 6. ELOGOFF: 对端进程调用了Stop
        if (cntlErrCode == ENOENT || cntlErrCode == EAGAIN ||
            cntlErrCode == EHOSTDOWN || cntlErrCode == ECONNREFUSED ||
            cntlErrCode == ECONNRESET || cntlErrCode == brpc::ELOGOFF) {
            // 当list中的所有节点都不可用时，failedAddr是0.0.0.0:0
            // 这里判断failedAdrr是否在chunkserverIpPorts中，如果不在，直接返回
            if (chunkserverIpPorts.count(cntlFailedAddr) == 0) {
                return -1;
            }

            chunkserverIpPorts.erase(cntlFailedAddr);
            cntlErrCode = 0;
            cntlFailedAddr.clear();
            continue;
        } else {
            return -1;
        }
    }

    return -1;
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

    LOG(INFO) << "user info [" << *user << "]";
    return true;
}
}   // namespace client
}   // namespace curve
