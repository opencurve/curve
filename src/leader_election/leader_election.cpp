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
 * Created Date: Tue June 25th 2019
 * Author: lixiaocui1
 */

#include "src/leader_election/leader_election.h"

#include <fiu.h>

#include <csignal>
#include <cstdlib>
#include <thread>  //NOLINT

#include "src/common/concurrent/concurrent.h"

using ::curve::common::Thread;

namespace curve {
namespace election {
int LeaderElection::CampaignLeader() {
    LOG(INFO) << opt_.leaderUniqueName
              << " start campaign leader prefix: " << realPrefix_;

    int resCode = opt_.etcdCli->CampaignLeader(
        realPrefix_, opt_.leaderUniqueName, opt_.sessionInterSec,
        opt_.electionTimeoutMs, &leaderOid_);
    if (resCode == EtcdErrCode::EtcdCampaignLeaderSuccess) {
        LOG(INFO) << opt_.leaderUniqueName
                  << " campaign leader prefix:" << realPrefix_ << " success";
        return 0;
    }

    LOG(WARNING) << opt_.leaderUniqueName
                 << " campaign leader prefix:" << realPrefix_
                 << " err: " << resCode;
    return -1;
}

void LeaderElection::StartObserverLeader() {
    Thread t(&LeaderElection::ObserveLeader, this);
    t.detach();
}

int LeaderElection::LeaderResign() {
    int res =
        opt_.etcdCli->LeaderResign(leaderOid_, 1000 * opt_.sessionInterSec);
    if (EtcdErrCode::EtcdLeaderResiginSuccess == res) {
        LOG(INFO) << opt_.leaderUniqueName
                  << " resign leader prefix:" << realPrefix_ << " ok";
        return 0;
    }

    LOG(WARNING) << opt_.leaderUniqueName
                 << " resign leader prefix:" << realPrefix_ << " err: " << res;
    return -1;
}

int LeaderElection::ObserveLeader() {
    LOG(INFO) << opt_.leaderUniqueName
              << " start observe for prefix:" << realPrefix_;
    int resCode =
        opt_.etcdCli->LeaderObserve(leaderOid_, opt_.leaderUniqueName);
    LOG(ERROR) << opt_.leaderUniqueName
               << " leader observe for prefix:" << realPrefix_
               << " occur error, errcode: " << resCode;

    // for test
    fiu_return_on("src/mds/leaderElection/observeLeader", -1);

    // Exit the current process
    LOG(INFO) << "mds is existing due to the error of leader observation";
    raise(SIGTERM);

    return -1;
}
}  // namespace election
}  // namespace curve
