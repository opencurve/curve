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

#ifndef CURVEFS_SRC_CLIENT_RPCCLIENT_CLI2_CLIENT_H_
#define CURVEFS_SRC_CLIENT_RPCCLIENT_CLI2_CLIENT_H_

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bthread/condition_variable.h>

#include <stdint.h>
#include <vector>
#include <string>
#include <memory>
#include "curvefs/proto/cli2.pb.h"
#include "curvefs/src/client/common/common.h"
#include "src/client/client_common.h"
#include "src/client/metacache_struct.h"

using ::curve::client::CopysetID;
using ::curve::client::CopysetInfo;
using ::curve::client::CopysetPeerInfo;
using ::curve::client::LogicPoolID;
using ::curve::client::PeerAddr;
using ::curvefs::client::common::MetaserverID;

namespace curvefs {
namespace client {
namespace rpcclient {

using PeerInfoList = std::vector<CopysetPeerInfo<MetaserverID>>;
using Task2 = std::function<void(brpc::Channel *channel)>;

class TaskExecutor2;
struct Cli2TaskContext {
    LogicPoolID poolID;
    CopysetID copysetID;
    std::string peerAddr;

    Cli2TaskContext(const LogicPoolID &poolid, const CopysetID &copysetid,
                    const std::string &peeraddr)
        : poolID(poolid), copysetID(copysetid), peerAddr(peeraddr) {}
};

class Cli2Closure : public google::protobuf::Closure {
 public:
    Cli2Closure() = default;
    explicit Cli2Closure(const Cli2TaskContext &context,
                         std::shared_ptr<TaskExecutor2> taskexcutor)
        : taskContext(context), excutor(taskexcutor) {}

    void Run() override;

 public:
    Cli2TaskContext taskContext;
    std::shared_ptr<TaskExecutor2> excutor;

    curvefs::metaserver::copyset::GetLeaderResponse2 response;
    brpc::Controller cntl;
};

struct Cli2ClientImplOption {
    uint32_t rpcTimeoutMs;

    explicit Cli2ClientImplOption(uint32_t rpcTimeoutMs = 500)
        : rpcTimeoutMs(rpcTimeoutMs) {}
};

class Cli2Client {
 public:
    Cli2Client() {}
    virtual ~Cli2Client() {}


    virtual bool GetLeader(const LogicPoolID &poolID,
                           const CopysetID &copysetID,
                           const PeerInfoList &peerInfoList,
                           int16_t currentLeaderIndex, PeerAddr *peerAddr,
                           MetaserverID *metaserverID) = 0;
};

class TaskExecutor2 {
 public:
    TaskExecutor2() : finish_(false), success_(false) {}

    bool DoRPCTaskAndWait(const Task2 &task, const std::string &peerAddr);

    void NotifyRpcFinish(bool success);

 private:
    bthread::ConditionVariable finishCv_;
    bthread::Mutex finishMtx_;

    bool finish_;
    bool success_;
};

class Cli2ClientImpl : public Cli2Client {
 public:
    Cli2ClientImpl() = default;
    explicit Cli2ClientImpl(const Cli2ClientImplOption &opt) : opt_(opt) {}

    bool GetLeader(const LogicPoolID &pooID, const CopysetID &copysetID,
                   const PeerInfoList &peerInfoList, int16_t currentLeaderIndex,
                   PeerAddr *peerAddr, MetaserverID *metaserverID) override;

 private:
    bool DoGetLeader(Cli2Closure *done, PeerAddr *peerAddr,
                     MetaserverID *metaserverID);

 private:
    Cli2ClientImplOption opt_;
};

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_CLI2_CLIENT_H_
