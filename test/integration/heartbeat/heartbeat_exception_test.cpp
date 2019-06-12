/*
 * Project: curve
 * Created Date: 2019-11-28
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "test/integration/heartbeat/common.h"

namespace curve {
namespace mds {
class HeartbeatExceptionTest : public ::testing::Test {
 protected:
    void InitConfiguration(Configuration *conf) {
        // db相关配置设置
        conf->SetStringValue("mds.DbName", "heartbeat_exception_test_mds");
        conf->SetStringValue("mds.DbUser", "root");
        conf->SetStringValue("mds.DbUrl", "localhost");
        conf->SetStringValue("mds.DbPassword", "qwer");
        conf->SetIntValue("mds.DbPoolSize", 16);
        conf->SetIntValue("mds.topology.ChunkServerStateUpdateSec", 0);

        // heartbeat相关配置设置
        conf->SetIntValue("mds.heartbeat.intervalMs", 100);
        conf->SetIntValue("mds.heartbeat.misstimeoutMs", 300);
        conf->SetIntValue("mds.heartbeat.offlinetimeoutMs", 500);
        conf->SetIntValue("mds.heartbeat.clean_follower_afterMs", sleepTimeMs_);

        // mds监听端口号
        conf->SetStringValue("mds.listen.addr", "127.0.0.1:6880");

        // scheduler相关的内容
        conf->SetBoolValue("mds.enable.copyset.scheduler", false);
        conf->SetBoolValue("mds.enable.leader.scheduler", false);
        conf->SetBoolValue("mds.enable.recover.scheduler", false);
        conf->SetBoolValue("mds.replica.replica.scheduler", false);

        conf->SetIntValue("mds.copyset.scheduler.intervalSec", 300);
        conf->SetIntValue("mds.leader.scheduler.intervalSec", 300);
        conf->SetIntValue("mds.recover.scheduler.intervalSec", 300);
        conf->SetIntValue("mds.replica.scheduler.intervalSec", 300);

        conf->SetIntValue("mds.schduler.operator.concurrent", 4);
        conf->SetIntValue("mds.schduler.transfer.limitSec", 10);
        conf->SetIntValue("mds.scheduler.add.limitSec", 10);
        conf->SetIntValue("mds.scheduler.remove.limitSec", 10);
        conf->SetDoubleValue("mds.scheduler.copysetNumRangePercent", 0.05);
        conf->SetDoubleValue("mds.schduler.scatterWidthRangePerent", 0.2);
        conf->SetIntValue("mds.scheduler.minScatterWidth", 50);
    }

    void BuildCopySetInfo(
        CopySetInfo *info, uint64_t epoch, ChunkServerIdType leader,
        const std::set<ChunkServerIdType> &members,
        ChunkServerIdType candidateId = UNINTIALIZE_ID) {
        info->SetEpoch(epoch);
        info->SetLeader(leader);
        info->SetCopySetMembers(members);
        if (candidateId != UNINTIALIZE_ID) {
            info->SetCandidate(candidateId);
        }
    }

    void SetUp() override {
        sleepTimeMs_ = 500;
        Configuration conf;
        InitConfiguration(&conf);
        hbtest_ = std::make_shared<HeartbeatIntegrationCommon>(conf);
        hbtest_->BuildBasicCluster();
    }

    void TearDown() {
        ASSERT_EQ(0, hbtest_->server_.Stop(100));
        ASSERT_EQ(0, hbtest_->server_.Join());
    }

 protected:
    std::shared_ptr<HeartbeatIntegrationCommon> hbtest_;
    int64_t sleepTimeMs_;
};

/*
* Jira: http://jira.netease.com/browse/CLDCFS-2009
*
* bug说明：稳定性测试环境，宕机一台机器之后设置pending，副本恢复过程中mds有切换
*         最终发现有5个pending状态的chunkserver没有完成迁移
* 分析：
* 1. mds1提供服务时产生operator并下发给copyset-1{A,B,C} + D的变更，C是offline状态
* 2. copyset-1完成配置变更，此时leader上的配置更新为epoch=2/{A,B,C,D},
*    candidate上的配置为epoch=1/{A,B,C}, mds1中记录的配置为epoch=1/{A,B,C}
* 3. mds1挂掉，mds2提供服务, 并从数据库加载copyset,mds2中copyset-1的配置
*    epoch=1/{A,B,C}
* 4. candidate-D上报心跳,copyset-1的配置为epoch=1/{A,B,C}。mds2发现D上报的
*    copyset中epoch和mds2记录的相同，但D并不在mds2记录的复制组中且调度模块也没有
*    对应的operator,下发命令把D上的copyset-1删除导致D被误删
*
* 解决方法：
* 正常情况下，heartbeat模块会在mds启动一定时间(目前配置20min)后才可以下发删除copyset
* 的命令，极大概率保证这段时间内copyset-leader上的配置更新到mds, 防止刚加入复制组
* 副本上的数据备误删
*
* 这个时间的起始点应该是mds正式对外提供服务的时间，而不是mds的启动时间。如果设置为mds的启动
* 时间，备mds启动很久后如果能够提供服务，就立马可以删除，导致bug
*/
TEST_F(HeartbeatExceptionTest, test_mdsRestart_opLost) {
    // 1. copyset-1(epoch=2, peers={1,2,3}, leader=1)
    //    scheduler中有+4的operator
    CopySetKey key{1, 1};
    int startEpoch = 2;
    ChunkServerIdType leader = 1;
    ChunkServerIdType candidate = 4;
    std::set<ChunkServerIdType> peers{1, 2, 3};
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    Operator op(2, key, OperatorPriority::NormalPriority,
        std::chrono::steady_clock::now(), std::make_shared<AddPeer>(4));
    op.timeLimit = std::chrono::seconds(3);
    hbtest_->AddOperatorToOpController(op);

    // 2. leader上报copyset-1(epoch=2, peers={1,2,3}, leader=1)
    //    mds下发配置变更
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(leader, &req);
    CopySetInfo csInfo(key.first, key.second);
    BuildCopySetInfo(&csInfo, startEpoch, leader, peers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response, 下发+D的配置变更
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(key.first, conf.logicalpoolid());
    ASSERT_EQ(key.second, conf.copysetid());
    ASSERT_EQ(peers.size(), conf.peers_size());
    ASSERT_EQ(startEpoch, conf.epoch());
    ASSERT_EQ(ConfigChangeType::ADD_PEER, conf.type());
    ASSERT_EQ("10.198.100.3:9001:0", conf.configchangeitem().address());

    // 3. 清除mds中的operrator(模拟mds重启)
    hbtest_->RemoveOperatorFromOpController(key);

    // 4. canndidate上报落后的与mds的配置(candidate回放日志时会一一apply旧配置):
    //    copyset-1(epoch=1, peers={1,2,3}, leader=1)
    //    由于mds.heartbeat.clean_follower_afterMs时间还没有到，mds还不能下发
    //    删除命令。mds下发为空，candidate上的数据不会被误删
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(candidate, &req);
    BuildCopySetInfo(&csInfo, startEpoch - 1, leader, peers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response, 为空
    ASSERT_EQ(0, rep.needupdatecopysets_size());

    // 5. 睡眠mds.heartbeat.clean_follower_afterMs + 10ms后
    //    canndidate上报staled copyset-1(epoch=1, peers={1,2,3}, leader=1)
    //    mds下发删除配置，candidate上的数据会被误删
    usleep((sleepTimeMs_ + 10) * 1000);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(candidate, &req);
    BuildCopySetInfo(&csInfo, startEpoch - 1, leader, peers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(key.first, conf.logicalpoolid());
    ASSERT_EQ(key.second, conf.copysetid());
    ASSERT_EQ(peers.size(), conf.peers_size());
    ASSERT_EQ(startEpoch, conf.epoch());

    // 6. leader上报最新配置copyset-1(epoch=3, peers={1,2,3,4}, leader=1)
    auto newPeers = peers;
    newPeers.emplace(candidate);
    auto newEpoch = startEpoch + 1;
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(leader, &req);
    BuildCopySetInfo(&csInfo, startEpoch + 1, leader, newPeers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response， 为空
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查mdstopology的数据
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(key, &copysetInfo));
    ASSERT_EQ(newEpoch, copysetInfo.GetEpoch());
    ASSERT_EQ(leader, copysetInfo.GetLeader());
    ASSERT_EQ(newPeers, copysetInfo.GetCopySetMembers());

    // 7. canndidate上报staled copyset-1(epoch=1, peers={1,2,3}, leader=1)
    //    mds不下发配置
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(candidate, &req);
    BuildCopySetInfo(&csInfo, startEpoch - 1, leader, peers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response, 下发copyset当前配置指导candidate删除数据
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

}  // namespace mds
}  // namespace curve
