/*
 * Project: curve
 * Created Date: 20190709
 * Author: lixiaocui1
 * Copyright (c) 2018 netease
 */

#include <fiu-control.h>
#include <gtest/gtest.h>
#include <memory>
#include "src/mds/leader_election/leader_election.h"
#include "test/mds/mock/mock_etcdclient.h"

using ::testing::_;
using ::testing::Return;

namespace curve {
namespace mds {

TEST(TestLeaderElection, test_leader_election) {
    auto client = std::make_shared<MockEtcdClient>();
    LeaderElectionOptions opts;
    opts.etcdCli = client;
    opts.leaderUniqueName = "leader1";
    opts.sessionInterSec = 1;
    opts.electionTimeoutMs = 0;
    auto leaderElection = std::make_shared<LeaderElection>(opts);
    fiu_init(0);
    fiu_enable("src/mds/leaderElection/observeLeader", 1, nullptr, 0);

    EXPECT_CALL(*client, CampaignLeader(_, _, _, _, _))
        .WillOnce(Return(EtcdErrCode::CampaignLeaderSuccess));
    ASSERT_EQ(0, leaderElection->CampaginLeader());

    EXPECT_CALL(*client, CampaignLeader(_, _, _, _, _))
        .WillOnce(Return(EtcdErrCode::CampaignInternalErr));
    ASSERT_EQ(-1, leaderElection->CampaginLeader());

    EXPECT_CALL(*client, LeaderResign(_, _))
        .WillOnce(Return(EtcdErrCode::LeaderResiginSuccess));
    ASSERT_EQ(0, leaderElection->LeaderResign());

    EXPECT_CALL(*client, LeaderResign(_, _))
        .WillOnce(Return(EtcdErrCode::LeaderResignErr));
    ASSERT_EQ(-1, leaderElection->LeaderResign());

    EXPECT_CALL(*client, LeaderObserve(_, _, _))
        .WillRepeatedly(Return(EtcdErrCode::ObserverLeaderInternal));
    ASSERT_EQ(-1, leaderElection->ObserveLeader());

    fiu_disable("src/mds/leaderElection/observeLeader");
}
}  // namespace mds
}  // namespace curve


