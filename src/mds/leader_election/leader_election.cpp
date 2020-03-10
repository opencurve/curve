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
    LOG(INFO) << opt_.leaderUniqueName << " start campaign leader prefix: "
        << opt_.campaginPrefix;

    int resCode = opt_.etcdCli->CampaignLeader(
        opt_.campaginPrefix,
        opt_.leaderUniqueName,
        opt_.sessionInterSec,
        opt_.electionTimeoutMs,
        &leaderOid_);
    if (resCode == EtcdErrCode::CampaignLeaderSuccess) {
        LOG(INFO) << opt_.leaderUniqueName << " campaign leader prefix:"
            << opt_.campaginPrefix << " success";
        return 0;
    }

    LOG(WARNING) << opt_.leaderUniqueName << " campaign leader prefix:"
        << opt_.campaginPrefix << " err: " << resCode;
    return -1;
}

void LeaderElection::StartObserverLeader() {
    Thread t(&LeaderElection::ObserveLeader, this);
    t.detach();
}

int LeaderElection::LeaderResign() {
    int res =
        opt_.etcdCli->LeaderResign(leaderOid_, 1000 * opt_.sessionInterSec);
    if (EtcdErrCode::LeaderResiginSuccess == res) {
        LOG(INFO) << opt_.leaderUniqueName << " resign leader prefix:"
            << opt_.campaginPrefix << " ok";
        return 0;
    }

    LOG(WARNING) << opt_.leaderUniqueName << " resign leader prefix:"
        << opt_.campaginPrefix << " err: " << res;
    return -1;
}

int LeaderElection::ObserveLeader() {
    LOG(INFO) << opt_.leaderUniqueName << " start observe for prefix:"
        << opt_.campaginPrefix;
    int resCode =
        opt_.etcdCli->LeaderObserve(leaderOid_, opt_.leaderUniqueName);
    LOG(ERROR) << opt_.leaderUniqueName << " leader observe for prefix:"
        << opt_.campaginPrefix << " occur error, errcode: " << resCode;

    // for test
    fiu_return_on("src/mds/leaderElection/observeLeader", -1);

    // 退出当前进程
    CHECK(false) << opt_.leaderUniqueName
        << " observer encounter error, leader exit for prefix:"
        << opt_.campaginPrefix;
}
}  // namespace mds
}  // namespace curve
