/*
 * Project: curve
 * Created Date: Tue June 25th 2019
 * Author: lixiaocui1
 * Copyright (c) 2018 netease
 */

#include <fiu.h>
#include <cstdlib>
#include <thread> //NOLINT
#include "src/mds/leader_election/leader_election.h"
#include "src/common/concurrent/concurrent.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"

using ::curve::common::Thread;

namespace curve {
namespace mds {
int LeaderElection::CampaginLeader() {
    LOG(INFO) << leaderName_ << " start campaign leader";
    int resCode = etcdCli_->CampaignLeader(
        LEADERCAMPAIGNNPFX, leaderName_, sessionInterSec_,
        electionTimeoutMs_, &leaderOid_);
    if (resCode == EtcdErrCode::CampaignLeaderSuccess) {
        LOG(INFO) << leaderName_ << " campaign leader success";
        return 0;
    }

    LOG(WARNING) << leaderName_ << " campaign leader err: " << resCode;
    return -1;
}

bool LeaderElection::LeaderKeyExist() {
    return etcdCli_->LeaderKeyExist(leaderOid_, observeTimeoutMs_);
}

void LeaderElection::StartObserverLeader() {
    Thread t(&LeaderElection::ObserveLeader, this);
    t.detach();
}

int LeaderElection::LeaderResign() {
    int res = etcdCli_->LeaderResign(leaderOid_, observeTimeoutMs_);
    if (EtcdErrCode::LeaderResiginSuccess == res) {
        LOG(INFO) << leaderName_ << " resign leader ok";
        return 0;
    }

    LOG(WARNING) << leaderName_ << " resign leader err: " << res;
    return -1;
}

int LeaderElection::ObserveLeader() {
    LOG(INFO) << leaderName_ << " start observe.";
    int resCode = etcdCli_->LeaderObserve(
        leaderOid_, observeTimeoutMs_, leaderName_);
    if (resCode == EtcdErrCode::ObserverLeaderInternal) {
        LOG(ERROR) << leaderName_ << " Observer channel closed permaturely";
    } else if (resCode == EtcdErrCode::ObserverLeaderChange) {
        LOG(WARNING) << leaderName_ << " Observer leader change";
    }

    // for test
    fiu_return_on("src/mds/leaderElection/observeLeader", -1);

    // 退出当前进程
    CHECK(false) << leaderName_ << " Observer encounter error, mds exit";
}
}  // namespace mds
}  // namespace curve
