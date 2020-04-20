/*
 * Project: curve
 * Created Date: Tue June 25th 2019
 * Author: lixiaocui1
 * Copyright (c) 2018 netease
 */

#include <fiu.h>
#include <cstdlib>
#include <thread> //NOLINT
#include "src/leader_election/leader_election.h"
#include "src/common/concurrent/concurrent.h"

using ::curve::common::Thread;

namespace curve {
namespace election {
int LeaderElection::CampaginLeader() {
    LOG(INFO) << opt_.leaderUniqueName << " start campaign leader prefix: "
        << realPrefix_;

    int resCode = opt_.etcdCli->CampaignLeader(
        realPrefix_,
        opt_.leaderUniqueName,
        opt_.sessionInterSec,
        opt_.electionTimeoutMs,
        &leaderOid_);
    if (resCode == EtcdErrCode::EtcdCampaignLeaderSuccess) {
        LOG(INFO) << opt_.leaderUniqueName << " campaign leader prefix:"
            << realPrefix_ << " success";
        return 0;
    }

    LOG(WARNING) << opt_.leaderUniqueName << " campaign leader prefix:"
        << realPrefix_ << " err: " << resCode;
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
        LOG(INFO) << opt_.leaderUniqueName << " resign leader prefix:"
            << realPrefix_ << " ok";
        return 0;
    }

    LOG(WARNING) << opt_.leaderUniqueName << " resign leader prefix:"
        << realPrefix_ << " err: " << res;
    return -1;
}

int LeaderElection::ObserveLeader() {
    LOG(INFO) << opt_.leaderUniqueName << " start observe for prefix:"
        << realPrefix_;
    int resCode =
        opt_.etcdCli->LeaderObserve(leaderOid_, opt_.leaderUniqueName);
    LOG(ERROR) << opt_.leaderUniqueName << " leader observe for prefix:"
        << realPrefix_ << " occur error, errcode: " << resCode;

    // for test
    fiu_return_on("src/mds/leaderElection/observeLeader", -1);

    // 退出当前进程
    CHECK(false) << opt_.leaderUniqueName
        << " observer encounter error, leader exit for prefix:"
        << realPrefix_;
}
}  // namespace election
}  // namespace curve
