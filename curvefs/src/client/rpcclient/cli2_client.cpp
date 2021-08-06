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
 * Created Date: Mon Sept 4 2021
 * Author: lixiaocui
 */

#include "curvefs/src/client/rpcclient/cli2_client.h"

namespace curvefs {
namespace client {
namespace rpcclient {

void Cli2Closure::Run() {
    bool success = false;
    if (cntl.Failed()) {
        LOG(WARNING) << "GetLeader failed from " << cntl.remote_side()
                     << ", logicpool id = " << taskContext.poolID
                     << ", copyset id = " << taskContext.copysetID
                     << ", error = " << cntl.ErrorText();
    } else {
        LOG(INFO) << "GetLeader returned from " << cntl.remote_side()
                  << ", logicpool id = " << taskContext.poolID
                  << ", copyset id = " << taskContext.copysetID
                  << ", leader = " << response.DebugString();
        success = true;
    }

    excutor->NotifyRpcFinish(success);
}

#define RPCFunc [&](brpc::Channel * channel) -> int  // NOLINT

bool Cli2ClientImpl::GetLeader(const LogicPoolID &poolID,
                               const CopysetID &copysetID,
                               const PeerInfoList &peerInfoList,
                               int16_t currentLeaderIndex, PeerAddr *peerAddr,
                               MetaserverID *metaserverID) {
    int16_t index = -1;
    peerAddr->Reset();

    bool getLeaderOK = false;
    // TODO(@lixiaocui): optimization with back up request
    for_each(peerInfoList.begin(), peerInfoList.end(),
             [&](const CopysetPeerInfo<MetaserverID> &info) {
                 ++index;

                 if (index == currentLeaderIndex || getLeaderOK) {
                     return;
                 }

                 std::string senderAddr(
                     butil::endpoint2str(info.externalAddr.addr_).c_str());

                 auto excutor = std::make_shared<TaskExecutor2>();
                 Cli2TaskContext taskCtx(poolID, copysetID, senderAddr);
                 Cli2Closure *done = new Cli2Closure(taskCtx, excutor);
                 done->cntl.set_timeout_ms(opt_.rpcTimeoutMs);

                 getLeaderOK = DoGetLeader(done, peerAddr, metaserverID);
             });

    return getLeaderOK;
}

bool Cli2ClientImpl::DoGetLeader(Cli2Closure *done, PeerAddr *peerAddr,
                                 MetaserverID *metaserverID) {
    std::unique_ptr<Cli2Closure> selfGuard(done);

    // define rpc task
    auto task = RPCFunc {
        curvefs::metaserver::copyset::GetLeaderRequest2 request;
        request.set_poolid(done->taskContext.poolID);
        request.set_copysetid(done->taskContext.copysetID);

        curvefs::metaserver::copyset::CliService2_Stub stub(channel);
        stub.GetLeader(&done->cntl, &request, &done->response, done);
    };

    // do rpc task
    bool ret =
        done->excutor->DoRPCTaskAndWait(task, done->taskContext.peerAddr);

    // handle response
    if (ret) {
        bool has_address = done->response.leader().has_address();
        if (has_address) {
            peerAddr->Parse(done->response.leader().address());
        }

        bool has_id = done->response.leader().has_id();
        if (has_id) {
            *metaserverID = done->response.leader().id();
        }
    } else {
        LOG(WARNING) << "get leader error for {poolid:"
                     << done->taskContext.poolID
                     << ", copysetid:" << done->taskContext.copysetID << "}";
    }

    return ret;
}

bool TaskExecutor2::DoRPCTaskAndWait(const Task2 &task,
                                     const std::string &peerAddr) {
    std::unique_ptr<brpc::Channel> channel(new brpc::Channel());
    int ret = channel->Init(peerAddr.c_str(), nullptr);
    if (ret != 0) {
        LOG(WARNING) << "GetLeader init channel to " << peerAddr << " failed";
        return false;
    }

    task(channel.get());

    // wait rpc done
    {
        std::unique_lock<bthread::Mutex> ulk(finishMtx_);
        while (!finish_) {
            finishCv_.wait(ulk);
        }
    }

    return success_;
}

void TaskExecutor2::NotifyRpcFinish(bool success) {
    std::lock_guard<bthread::Mutex> ulk(finishMtx_);
    finish_ = true;
    success_ = success;
    finishCv_.notify_one();
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
