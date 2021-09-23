/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-09-12
 * Author: chenwei
 */

#include <braft/closure_helper.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <list>
#include <memory>
#include <vector>

#include "curvefs/src/metaserver/copyset/utils.h"
#include "curvefs/src/metaserver/heartbeat.h"
#include "src/common/timeutility.h"
#include "src/common/uri_parser.h"

namespace curvefs {
namespace metaserver {
int Heartbeat::Init(const HeartbeatOptions& options) {
    toStop_.store(false, std::memory_order_release);
    options_ = options;

    butil::ip_t msIp;
    if (butil::str2ip(options_.ip.c_str(), &msIp) < 0) {
        LOG(ERROR) << "Invalid Metaserver IP provided: " << options_.ip;
        return -1;
    }
    msEp_ = butil::EndPoint(msIp, options_.port);
    LOG(INFO) << "Metaserver address: " << options_.ip << ":" << options_.port;

    // mdsEps can not empty
    ::curve::common::SplitString(options_.mdsListenAddr, ",", &mdsEps_);
    if (mdsEps_.empty()) {
        LOG(ERROR) << "Invalid mds ip provided: " << options_.mdsListenAddr;
        return -1;
    }

    // Check the legitimacy of each address
    for (const auto &addr : mdsEps_) {
        butil::EndPoint endpt;
        if (butil::str2endpoint(addr.c_str(), &endpt) < 0) {
            LOG(ERROR) << "Invalid sub mds ip:port provided: " << addr;
            return -1;
        }
    }

    inServiceIndex_ = 0;
    LOG(INFO) << "MDS address: " << options_.mdsListenAddr;

    copysetMan_ = options.copysetNodeManager;

    LOG(INFO) << "MDS timer: " << options_.intervalSec << " seconde";
    waitInterval_.Init(options_.intervalSec * 1000);

    startUpTime_ = ::curve::common::TimeUtility::GetTimeofDaySec();
    return 0;
}

int Heartbeat::Run() {
    hbThread_ = Thread(&Heartbeat::HeartbeatWorker, this);
    return 0;
}

int Heartbeat::Stop() {
    LOG(INFO) << "Stopping Heartbeat manager.";

    waitInterval_.StopWait();
    toStop_.store(true, std::memory_order_release);
    hbThread_.join();

    LOG(INFO) << "Stopped Heartbeat manager.";
    return 0;
}

int Heartbeat::Fini() {
    Stop();

    LOG(INFO) << "Heartbeat manager cleaned up.";
    return 0;
}

void Heartbeat::BuildCopysetInfo(curvefs::mds::heartbeat::CopySetInfo* info,
                                CopysetNode* copyset) {
    int ret;
    PoolId poolId = copyset->GetPoolId();
    CopysetId copysetId = copyset->GetCopysetId();

    info->set_poolid(poolId);
    info->set_copysetid(copysetId);
    info->set_epoch(copyset->GetConfEpoch());

    std::vector<Peer> peers;
    copyset->ListPeers(&peers);
    for (Peer peer : peers) {
        auto replica = info->add_peers();
        replica->set_address(peer.address().c_str());
    }

    PeerId leader = copyset->GetLeaderId();
    Peer* replica = new Peer();
    replica->set_address(leader.to_string());
    info->set_allocated_leaderpeer(replica);

    // add partition info
    for (auto it : copyset->GetPartitionInfoList()) {
        info->add_partitioninfolist()->CopyFrom(it);
    }

    // TODO(cw123) : when add schedule, add copyset config change infos
    return;
}

void Heartbeat::BuildRequest(HeartbeatRequest* req) {
    int ret;

    req->set_metaserverid(options_.metaserverId);
    req->set_token(options_.metaserverToken);
    req->set_starttime(startUpTime_);
    req->set_ip(options_.ip);
    req->set_port(options_.port);

    std::vector<CopysetNode*> copysets;
    copysetMan_->GetAllCopysets(&copysets);

    req->set_copysetcount(copysets.size());
    int leaders = 0;

    for (auto copyset : copysets) {
        curvefs::mds::heartbeat::CopySetInfo* info = req->add_copysetinfos();

        BuildCopysetInfo(info, copyset);

        if (copyset->IsLeaderTerm()) {
            ++leaders;
        }
    }
    req->set_leadercount(leaders);

    return;
}

void Heartbeat::DumpHeartbeatRequest(const HeartbeatRequest& request) {
    DVLOG(6) << "Heartbeat request: Metaserver ID: " << request.metaserverid()
             << ", IP: " << request.ip() << ", port: " << request.port()
             << ", copyset count: " << request.copysetcount()
             << ", leader count: " << request.leadercount();
    for (int i = 0; i < request.copysetinfos_size(); i++) {
        const curvefs::mds::heartbeat::CopySetInfo& info =
            request.copysetinfos(i);

        std::string peersStr = "";
        for (int j = 0; j < info.peers_size(); j++) {
            peersStr += info.peers(j).address() + ",";
        }

        DVLOG(6) << "Copyset " << i << " "
                 << copyset::ToGroupIdString(info.poolid(), info.copysetid())
                 << ", epoch: " << info.epoch()
                 << ", leader: " << info.leaderpeer().address()
                 << ", peers: " << peersStr;
    }
}

void Heartbeat::DumpHeartbeatResponse(const HeartbeatResponse& response) {
    LOG(INFO) << "Received heartbeat response, statusCode = "
              << response.statuscode();
}

int Heartbeat::SendHeartbeat(const HeartbeatRequest& request,
                             HeartbeatResponse* response) {
    brpc::Channel channel;
    if (channel.Init(mdsEps_[inServiceIndex_].c_str(), NULL) != 0) {
        LOG(ERROR) << msEp_.ip << ":" << msEp_.port
                   << " Fail to init channel to MDS "
                   << mdsEps_[inServiceIndex_];
        return -1;
    }

    curvefs::mds::heartbeat::HeartbeatService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(options_.timeout);

    DumpHeartbeatRequest(request);

    stub.MetaServerHeartbeat(&cntl, &request, response, nullptr);
    if (cntl.Failed()) {
        if (cntl.ErrorCode() == EHOSTDOWN || cntl.ErrorCode() == ETIMEDOUT ||
            cntl.ErrorCode() == brpc::ELOGOFF ||
            cntl.ErrorCode() == brpc::ERPCTIMEDOUT) {
            LOG(WARNING) << "current mds: " << mdsEps_[inServiceIndex_]
                         << " is shutdown or going to quit";
            inServiceIndex_ = (inServiceIndex_ + 1) % mdsEps_.size();
            LOG(INFO) << "next heartbeat switch to "
                      << mdsEps_[inServiceIndex_];
        } else {
            LOG(ERROR) << msEp_.ip << ":" << msEp_.port
                       << " Fail to send heartbeat to MDS "
                       << mdsEps_[inServiceIndex_] << ","
                       << " cntl errorCode: " << cntl.ErrorCode()
                       << " cntl error: " << cntl.ErrorText();
        }
        return -1;
    } else {
        DumpHeartbeatResponse(*response);
    }

    return 0;
}

void Heartbeat::HeartbeatWorker() {
    int ret;
    int errorIntervalSec = 2;

    LOG(INFO) << "Starting Heartbeat worker thread.";

    // Handling abnormal situations such as conf equal to 0
    if (options_.intervalSec <= 4) {
        errorIntervalSec = 2;
    } else {
        errorIntervalSec = options_.intervalSec / 2;
    }

    while (!toStop_.load(std::memory_order_acquire)) {
        HeartbeatRequest req;
        HeartbeatResponse resp;

        LOG(INFO) << "building heartbeat info";
        BuildRequest(&req);

        LOG(INFO) << "sending heartbeat info";
        ret = SendHeartbeat(req, &resp);
        if (ret != 0) {
            LOG(WARNING) << "Failed to send heartbeat to MDS";
            ::sleep(errorIntervalSec);
            continue;
        }

        // TODO(cw123): add schedule and execute heartbeat info

        waitInterval_.WaitForNextExcution();
    }

    LOG(INFO) << "Heartbeat worker thread stopped.";
}

}  // namespace metaserver
}  // namespace curvefs
