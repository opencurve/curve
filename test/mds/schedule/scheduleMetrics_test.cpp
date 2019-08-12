/*
 * Project: curve
 * Created Date: 20190704
 * Author: lixiaocui
 * Copyright (c) 2019 netease
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json2pb/json_to_pb.h>
#include "src/mds/schedule/scheduleMetrics.h"
#include "src/mds/schedule/operatorController.h"
#include "test/mds/mock/mock_topology.h"

using ::curve::mds::topology::MockTopology;
using ::curve::mds::topology::CopySetKey;

using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::SetArgPointee;
using ::testing::DoAll;

namespace curve {
namespace mds {
namespace schedule {
class ScheduleMetricsTest : public testing::Test {
 public:
    void SetUp() {
        topo = std::make_shared<MockTopology>();
        scheduleMetrics = std::make_shared<ScheduleMetrics>(topo);
    }

    void TearDown() {
    }

    ::curve::mds::topology::ChunkServer GetChunkServer(int id) {
        return ::curve::mds::topology::ChunkServer(
            id, "", "", id, "", 9000, "");
    }

    ::curve::mds::topology::Server GetServer(int id) {
        std::string hostName =
            "pubbeta2-curve" + std::to_string(id) + ".dg.163.org";
        return ::curve::mds::topology::Server(
            id, hostName, "", 0, "", 0, id, 1, "");
    }

    std::string GetChunkServerHostPort(int id) {
        return GetServer(id).GetHostName() + ":" +
            std::to_string(GetChunkServer(id).GetPort());
    }

 public:
    std::shared_ptr<ScheduleMetrics> scheduleMetrics;
    std::shared_ptr<MockTopology> topo;
};

TEST_F(ScheduleMetricsTest, test_add_rm_addOp) {
    Operator addOp(1, CopySetKey{1, 1}, OperatorPriority::NormalPriority,
                steady_clock::now(), std::make_shared<AddPeer>(3));

    ::curve::mds::topology::CopySetInfo addCsInfo(1, 1);
    addCsInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2});

    {
        // 1. 增加normal级别/add类型的operator
        EXPECT_CALL(*topo, GetCopySet(CopySetKey{1, 1}, _))
            .WillOnce(DoAll(SetArgPointee<1>(addCsInfo), Return(true)));
        EXPECT_CALL(*topo, GetChunkServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetChunkServer(1)), Return(true)));
        EXPECT_CALL(*topo, GetChunkServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetChunkServer(2)), Return(true)));
        EXPECT_CALL(*topo, GetChunkServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetChunkServer(3)), Return(true)));
        EXPECT_CALL(*topo, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(1)), Return(true)));
        EXPECT_CALL(*topo, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(2)), Return(true)));
        EXPECT_CALL(*topo, GetServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(3)), Return(true)));

        scheduleMetrics->UpdateAddMetric(addOp);
        ASSERT_EQ(1, scheduleMetrics->operatorNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->addOpNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->normalOpNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->operators.size());

        ASSERT_EQ(std::to_string(addCsInfo.GetLogicalPoolId()),
            scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                "logicalPoolId"));
        ASSERT_EQ(std::to_string(addCsInfo.GetId()),
            scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                "copySetId"));
        std::string copysetpeers =
            GetChunkServerHostPort(1) + "," + GetChunkServerHostPort(2);
        ASSERT_EQ(copysetpeers,
            scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                "copySetPeers"));
        ASSERT_EQ(std::to_string(addCsInfo.GetEpoch()),
            scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                "copySetEpoch"));
        ASSERT_EQ("UNINTIALIZE_ID",
            scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                "copySetLeader"));
        ASSERT_EQ(std::to_string(addOp.startEpoch),
            scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                "startEpoch"));
        ASSERT_EQ(NORMAL,
            scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                "opPriority"));
        ASSERT_EQ(ADDPEER,
            scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                "opType"));
        ASSERT_EQ(GetChunkServerHostPort(3),
            scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                "opItem"));
        std::string res =
            std::string("{\"copySetEpoch\":\"0\",\"copySetId\":\"1\",") +
            std::string("\"copySetLeader\":\"UNINTIALIZE_ID\",\"") +
            std::string("copySetPeers\":\"pubbeta2-curve1.dg.163.org:9000,") +
            std::string("pubbeta2-curve2.dg.163.org:9000\",\"") +
            std::string("logicalPoolId\":\"1\",\"opItem\":\"") +
            std::string("pubbeta2-curve3.dg.163.org:9000\",\"opPriority\":") +
            std::string("\"Normal\",\"opType\":\"AddPeer\",") +
            std::string("\"startEpoch\":\"1\"}");
        ASSERT_EQ(res, scheduleMetrics->operators[addOp.copysetID].JsonBody());
        LOG(INFO) << "format: "
                  << scheduleMetrics->operators[addOp.copysetID].JsonBody();
    }

    {
        // 2. 移除 1中的operator
        scheduleMetrics->UpdateRemoveMetric(addOp);
        ASSERT_EQ(0, scheduleMetrics->operatorNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->addOpNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->normalOpNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->operators.size());
    }
}

TEST_F(ScheduleMetricsTest, test_add_rm_rmOp) {
    Operator rmOp(1, CopySetKey{1, 2}, OperatorPriority::HighPriority,
                steady_clock::now(), std::make_shared<RemovePeer>(3));
    ::curve::mds::topology::CopySetInfo rmCsInfo(1, 2);
    rmCsInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    rmCsInfo.SetLeader(1);

    {
        // 1. 增加high级别/remove类型的operator
        EXPECT_CALL(*topo, GetCopySet(CopySetKey{1, 2}, _))
            .WillOnce(DoAll(SetArgPointee<1>(rmCsInfo), Return(true)));
        EXPECT_CALL(*topo, GetChunkServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetChunkServer(1)), Return(true)));
        EXPECT_CALL(*topo, GetChunkServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetChunkServer(2)), Return(true)));
        EXPECT_CALL(*topo, GetChunkServer(3, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(GetChunkServer(3)),
                            Return(true)));
        EXPECT_CALL(*topo, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(1)), Return(true)));
        EXPECT_CALL(*topo, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(2)), Return(true)));
        EXPECT_CALL(*topo, GetServer(3, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(GetServer(3)),
                                  Return(true)));

        scheduleMetrics->UpdateAddMetric(rmOp);
        ASSERT_EQ(1, scheduleMetrics->operatorNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->removeOpNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->highOpNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->operators.size());

        ASSERT_EQ(std::to_string(rmCsInfo.GetLogicalPoolId()),
            scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                "logicalPoolId"));
        ASSERT_EQ(std::to_string(rmCsInfo.GetId()),
            scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                "copySetId"));
        std::string copysetpeers = GetChunkServerHostPort(1) + "," +
            GetChunkServerHostPort(2) + "," + GetChunkServerHostPort(3);
        ASSERT_EQ(copysetpeers,
            scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                "copySetPeers"));
        ASSERT_EQ(std::to_string(rmCsInfo.GetEpoch()),
            scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                "copySetEpoch"));
        ASSERT_EQ(GetChunkServerHostPort(1),
            scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                "copySetLeader"));
        ASSERT_EQ(std::to_string(rmOp.startEpoch),
            scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                "startEpoch"));
        ASSERT_EQ(HIGH,
            scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                "opPriority"));
        ASSERT_EQ(REMOVEPEER,
            scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                "opType"));
        ASSERT_EQ(GetChunkServerHostPort(3),
            scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                "opItem"));
        std::string res =
            std::string("{\"copySetEpoch\":\"0\",\"copySetId\":\"2\",") +
            std::string("\"copySetLeader\":") +
            std::string("\"pubbeta2-curve1.dg.163.org:9000\",\"") +
            std::string("copySetPeers\":\"pubbeta2-curve1.dg.163.org:9000") +
            std::string(",pubbeta2-curve2.dg.163.org:9000,") +
            std::string("pubbeta2-curve3.dg.163.org:9000\",\"logicalPoolId") +
            std::string("\":\"1\",\"opItem\":\"") +
            std::string("pubbeta2-curve3.dg.163.org:9000\",\"opPriority\":") +
            std::string("\"High\",\"opType\":\"RemovePeer\",") +
            std::string("\"startEpoch\":\"1\"}");
        ASSERT_EQ(res, scheduleMetrics->operators[rmOp.copysetID].JsonBody());
        LOG(INFO) << "format: "
                  << scheduleMetrics->operators[rmOp.copysetID].JsonBody();
    }

    {
        // 2. 移除 1中的operator
        scheduleMetrics->UpdateRemoveMetric(rmOp);
        ASSERT_EQ(0, scheduleMetrics->operatorNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->removeOpNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->highOpNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->operators.size());
    }
}

TEST_F(ScheduleMetricsTest, test_add_rm_transferOp) {
    Operator transferOp(1, CopySetKey{1, 3}, OperatorPriority::NormalPriority,
            steady_clock::now(), std::make_shared<TransferLeader>(1, 3));
    ::curve::mds::topology::CopySetInfo transCsInfo(1, 3);
    transCsInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    transCsInfo.SetLeader(1);

    {
        // 1. 增加normal级别/transferleader类型的operator
        EXPECT_CALL(*topo, GetCopySet(CopySetKey{1, 3}, _))
            .WillOnce(DoAll(SetArgPointee<1>(transCsInfo), Return(true)));
        EXPECT_CALL(*topo, GetChunkServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetChunkServer(1)), Return(true)));
        EXPECT_CALL(*topo, GetChunkServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetChunkServer(2)), Return(true)));
        EXPECT_CALL(*topo, GetChunkServer(3, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(GetChunkServer(3)),
                            Return(true)));
        EXPECT_CALL(*topo, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(1)), Return(true)));
        EXPECT_CALL(*topo, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(2)), Return(true)));
        EXPECT_CALL(*topo, GetServer(3, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(GetServer(3)),
                                  Return(true)));

        scheduleMetrics->UpdateAddMetric(transferOp);
        ASSERT_EQ(1, scheduleMetrics->operatorNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->normalOpNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->transferOpNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->operators.size());

        ASSERT_EQ(std::to_string(transCsInfo.GetLogicalPoolId()),
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "logicalPoolId"));
         ASSERT_EQ(std::to_string(transCsInfo.GetId()),
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "copySetId"));
        std::string copysetpeers = GetChunkServerHostPort(1) + "," +
            GetChunkServerHostPort(2) + "," + GetChunkServerHostPort(3);
        ASSERT_EQ(copysetpeers,
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "copySetPeers"));
        ASSERT_EQ(std::to_string(transCsInfo.GetEpoch()),
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "copySetEpoch"));
        ASSERT_EQ(std::to_string(transferOp.startEpoch),
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "startEpoch"));
        ASSERT_EQ(NORMAL,
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "opPriority"));
        ASSERT_EQ(TRANSFERLEADER,
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "opType"));
        ASSERT_EQ(GetChunkServerHostPort(3),
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "opItem"));
        std::string res =
            std::string("{\"copySetEpoch\":\"0\",\"copySetId\":\"3\",") +
            std::string("\"copySetLeader\":") +
            std::string("\"pubbeta2-curve1.dg.163.org:9000\",\"") +
            std::string("copySetPeers\":\"pubbeta2-curve1.dg.163.org:9000") +
            std::string(",pubbeta2-curve2.dg.163.org:9000,") +
            std::string("pubbeta2-curve3.dg.163.org:9000\",\"logicalPoolId") +
            std::string("\":\"1\",\"opItem\":\"") +
            std::string("pubbeta2-curve3.dg.163.org:9000\",\"opPriority\":") +
            std::string("\"Normal\",\"opType\":\"TransferLeader\",") +
            std::string("\"startEpoch\":\"1\"}");
        ASSERT_EQ(res,
            scheduleMetrics->operators[transferOp.copysetID].JsonBody());
        LOG(INFO) << "format: "
            << scheduleMetrics->operators[transferOp.copysetID].JsonBody();
    }

    {
        // 2. 移除 1中的operator
        scheduleMetrics->UpdateRemoveMetric(transferOp);
        ASSERT_EQ(0, scheduleMetrics->operatorNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->transferOpNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->normalOpNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->operators.size());

        // 移除map中不存在的metric应该没有问题
        scheduleMetrics->UpdateRemoveMetric(transferOp);
    }
}

TEST_F(ScheduleMetricsTest, test_abnormal) {
    Operator transferOp(1, CopySetKey{1, 3}, OperatorPriority::NormalPriority,
            steady_clock::now(), std::make_shared<TransferLeader>(1, 3));
    ::curve::mds::topology::CopySetInfo transCsInfo(1, 3);
    transCsInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    transCsInfo.SetLeader(1);

    // 获取copyset失败
    EXPECT_CALL(*topo, GetCopySet(CopySetKey{1, 3}, _)).WillOnce(Return(false));
    scheduleMetrics->UpdateAddMetric(transferOp);
    ASSERT_EQ(1, scheduleMetrics->operatorNum.get_value());
    ASSERT_EQ(1, scheduleMetrics->transferOpNum.get_value());
    ASSERT_EQ(1, scheduleMetrics->normalOpNum.get_value());
    ASSERT_EQ(1, scheduleMetrics->operators.size());
    ASSERT_TRUE(
        scheduleMetrics->operators[transferOp.copysetID].JsonBody().empty());
    LOG(INFO) << "format: "
            << scheduleMetrics->operators[transferOp.copysetID].JsonBody();
    scheduleMetrics->UpdateRemoveMetric(transferOp);


    // 获取chunkserver 或者 server失败
    EXPECT_CALL(*topo, GetCopySet(CopySetKey{1, 3}, _))
            .WillOnce(DoAll(SetArgPointee<1>(transCsInfo), Return(true)));
    EXPECT_CALL(*topo, GetChunkServer(1, _)).WillOnce(Return(false));
    EXPECT_CALL(*topo, GetChunkServer(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(GetChunkServer(2)), Return(true)));
    EXPECT_CALL(*topo, GetChunkServer(3, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(GetChunkServer(3)),
                        Return(true)));
    EXPECT_CALL(*topo, GetServer(2, _))
        .WillOnce(Return(false));
    EXPECT_CALL(*topo, GetServer(3, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(GetServer(3)),
                              Return(true)));
    scheduleMetrics->UpdateAddMetric(transferOp);
    std::string res =
            std::string("{\"copySetEpoch\":\"0\",\"copySetId\":\"3\",") +
            std::string("\"copySetLeader\":\"UNINTIALIZE_ID\",\"") +
            std::string("copySetPeers\":\",,pubbeta2-curve3.dg.163.org:9000") +
            std::string("\",\"logicalPoolId\":\"1\",\"opItem\":") +
            std::string("\"pubbeta2-curve3.dg.163.org:9000\",\"opPriority\":") +
            std::string("\"Normal\",\"opType\":\"TransferLeader\",") +
            std::string("\"startEpoch\":\"1\"}");
    ASSERT_EQ(res, scheduleMetrics->operators[transferOp.copysetID].JsonBody());
    LOG(INFO) << "format: "
            << scheduleMetrics->operators[transferOp.copysetID].JsonBody();
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

