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
 * @Project: curve
 * @Date: 2021-11-15 11:01:48
 * @Author: chenwei
 */

#include "curvefs/src/mds/schedule/scheduleMetrics.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json2pb/json_to_pb.h>

#include "curvefs/src/mds/schedule/operatorController.h"
#include "curvefs/test/mds/mock/mock_topology.h"

using ::curvefs::mds::topology::CopySetKey;
using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::TopologyIdGenerator;
using ::curvefs::mds::topology::TopologyStorage;
using ::curvefs::mds::topology::TopologyTokenGenerator;

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

namespace curvefs {
namespace mds {
namespace schedule {
class ScheduleMetricsTest : public testing::Test {
 public:
    void SetUp() {
        std::shared_ptr<TopologyIdGenerator> idGenerator;
        std::shared_ptr<TopologyTokenGenerator> tokenGenerator;
        std::shared_ptr<TopologyStorage> storage;
        topo = std::make_shared<MockTopology>(idGenerator, tokenGenerator,
                                              storage);
        scheduleMetrics = std::make_shared<ScheduleMetrics>(topo);
    }

    void TearDown() {}

    ::curvefs::mds::topology::MetaServer GetMetaServer(int id) {
        return ::curvefs::mds::topology::MetaServer(id, "hostname", "token", id,
                                                    "ip", 9000, "ip", 9000);
    }

    ::curvefs::mds::topology::Server GetServer(int id) {
        std::string hostName = "hostname" + std::to_string(id);
        return ::curvefs::mds::topology::Server(id, hostName, "", 0, "", 0, id,
                                                1);
    }

    std::string GetMetaServerHostPort(int id) {
        return GetServer(id).GetHostName() + ":" +
               std::to_string(GetMetaServer(id).GetInternalPort());
    }

 public:
    std::shared_ptr<ScheduleMetrics> scheduleMetrics;
    std::shared_ptr<MockTopology> topo;
};

TEST_F(ScheduleMetricsTest, test_add_rm_addOp) {
    Operator addOp(1, CopySetKey{1, 1}, OperatorPriority::NormalPriority,
                   steady_clock::now(), std::make_shared<AddPeer>(3));
    ::curvefs::mds::topology::CopySetInfo addCsInfo(1, 1);
    addCsInfo.SetCopySetMembers(std::set<MetaServerIdType>{1, 2});
    {
        // 1. Add operator of normal level/add type
        EXPECT_CALL(*topo, GetCopySet(CopySetKey{1, 1}, _))
            .WillOnce(DoAll(SetArgPointee<1>(addCsInfo), Return(true)));
        EXPECT_CALL(*topo, GetMetaServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetMetaServer(1)), Return(true)));
        EXPECT_CALL(*topo, GetMetaServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetMetaServer(2)), Return(true)));
        EXPECT_CALL(*topo, GetMetaServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetMetaServer(3)), Return(true)));
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

        ASSERT_EQ(std::to_string(addCsInfo.GetPoolId()),
                  scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                      "poolId"));
        ASSERT_EQ(std::to_string(addCsInfo.GetId()),
                  scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                      "copySetId"));
        std::string copysetpeers =
            GetMetaServerHostPort(1) + "," + GetMetaServerHostPort(2);
        ASSERT_EQ(copysetpeers,
                  scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                      "copySetPeers"));
        ASSERT_EQ(std::to_string(addCsInfo.GetEpoch()),
                  scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                      "copySetEpoch"));
        ASSERT_EQ("UNINITIALIZE_ID",
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
        ASSERT_EQ(GetMetaServerHostPort(3),
                  scheduleMetrics->operators[addOp.copysetID].GetValueByKey(
                      "opItem"));
        std::string res =
            std::string("{\"copySetEpoch\":\"0\",\"copySetId\":\"1\",") +
            std::string("\"copySetLeader\":\"UNINITIALIZE_ID\",\"") +
            std::string("copySetPeers\":\"hostname1:9000,") +
            std::string(
                "hostname2:9000\",\""
                "opItem\":\"") +
            std::string("hostname3:9000\",\"opPriority\":") +
            std::string("\"Normal\",\"opType\":\"AddPeer\",") +
            std::string("\"poolId\":\"1\",\"startEpoch\":\"1\"}");

        ASSERT_EQ(res, scheduleMetrics->operators[addOp.copysetID].JsonBody());
        LOG(INFO) << "format: "
                  << scheduleMetrics->operators[addOp.copysetID].JsonBody();
    }

    {
        // 2. Remove operator from 1
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
    ::curvefs::mds::topology::CopySetInfo rmCsInfo(1, 2);
    rmCsInfo.SetCopySetMembers(std::set<MetaServerIdType>{1, 2, 3});
    rmCsInfo.SetLeader(1);

    {
        // 1. Add high level/remove type operators
        EXPECT_CALL(*topo, GetCopySet(CopySetKey{1, 2}, _))
            .WillOnce(DoAll(SetArgPointee<1>(rmCsInfo), Return(true)));
        EXPECT_CALL(*topo, GetHostNameAndPortById(_)).WillOnce(Return("haha"));

        EXPECT_CALL(*topo, GetMetaServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetMetaServer(1)), Return(true)));
        EXPECT_CALL(*topo, GetMetaServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetMetaServer(2)), Return(true)));
        EXPECT_CALL(*topo, GetMetaServer(3, _))
            .Times(2)
            .WillRepeatedly(
                DoAll(SetArgPointee<1>(GetMetaServer(3)), Return(true)));
        EXPECT_CALL(*topo, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(1)), Return(true)));
        EXPECT_CALL(*topo, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(2)), Return(true)));
        EXPECT_CALL(*topo, GetServer(3, _))
            .Times(2)
            .WillRepeatedly(
                DoAll(SetArgPointee<1>(GetServer(3)), Return(true)));

        scheduleMetrics->UpdateAddMetric(rmOp);
        ASSERT_EQ(1, scheduleMetrics->operatorNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->removeOpNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->highOpNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->operators.size());

        ASSERT_EQ(
            std::to_string(rmCsInfo.GetPoolId()),
            scheduleMetrics->operators[rmOp.copysetID].GetValueByKey("poolId"));
        ASSERT_EQ(std::to_string(rmCsInfo.GetId()),
                  scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                      "copySetId"));
        std::string copysetpeers = GetMetaServerHostPort(1) + "," +
                                   GetMetaServerHostPort(2) + "," +
                                   GetMetaServerHostPort(3);
        ASSERT_EQ(copysetpeers,
                  scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                      "copySetPeers"));
        ASSERT_EQ(std::to_string(rmCsInfo.GetEpoch()),
                  scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                      "copySetEpoch"));
        ASSERT_EQ(GetMetaServerHostPort(1),
                  scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                      "copySetLeader"));
        ASSERT_EQ(std::to_string(rmOp.startEpoch),
                  scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                      "startEpoch"));
        ASSERT_EQ(HIGH,
                  scheduleMetrics->operators[rmOp.copysetID].GetValueByKey(
                      "opPriority"));
        ASSERT_EQ(
            REMOVEPEER,
            scheduleMetrics->operators[rmOp.copysetID].GetValueByKey("opType"));
        ASSERT_EQ(
            GetMetaServerHostPort(3),
            scheduleMetrics->operators[rmOp.copysetID].GetValueByKey("opItem"));

        std::string res =
            std::string("{\"copySetEpoch\":\"0\",\"copySetId\":\"2\",") +
            std::string("\"copySetLeader\":") +
            std::string("\"hostname1:9000\",\"") +
            std::string("copySetPeers\":\"hostname1:9000") +
            std::string(",hostname2:9000,") +
            std::string("hostname3:9000\",\"opItem\":\"") +
            std::string("hostname3:9000\",\"opPriority\":") +
            std::string("\"High\",\"opType\":\"RemovePeer\",") +
            std::string("\"poolId\":\"1\",") +
            std::string("\"startEpoch\":\"1\"}");

        ASSERT_EQ(res, scheduleMetrics->operators[rmOp.copysetID].JsonBody());
        LOG(INFO) << "format: "
                  << scheduleMetrics->operators[rmOp.copysetID].JsonBody();
    }

    {
        // 2. Remove operator from 1
        scheduleMetrics->UpdateRemoveMetric(rmOp);
        ASSERT_EQ(0, scheduleMetrics->operatorNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->removeOpNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->highOpNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->operators.size());
    }
}

TEST_F(ScheduleMetricsTest, test_add_rm_transferOp) {
    Operator transferOp(1, CopySetKey{1, 3}, OperatorPriority::NormalPriority,
                        steady_clock::now(),
                        std::make_shared<TransferLeader>(1, 3));
    ::curvefs::mds::topology::CopySetInfo transCsInfo(1, 3);
    transCsInfo.SetCopySetMembers(std::set<MetaServerIdType>{1, 2, 3});
    transCsInfo.SetLeader(1);

    {
        // 1. Increase the operator of the normal level/transferleader type
        EXPECT_CALL(*topo, GetCopySet(CopySetKey{1, 3}, _))
            .WillOnce(DoAll(SetArgPointee<1>(transCsInfo), Return(true)));
        EXPECT_CALL(*topo, GetMetaServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetMetaServer(1)), Return(true)));
        EXPECT_CALL(*topo, GetMetaServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetMetaServer(2)), Return(true)));
        EXPECT_CALL(*topo, GetMetaServer(3, _))
            .Times(2)
            .WillRepeatedly(
                DoAll(SetArgPointee<1>(GetMetaServer(3)), Return(true)));
        EXPECT_CALL(*topo, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(1)), Return(true)));
        EXPECT_CALL(*topo, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(2)), Return(true)));
        EXPECT_CALL(*topo, GetServer(3, _))
            .Times(2)
            .WillRepeatedly(
                DoAll(SetArgPointee<1>(GetServer(3)), Return(true)));

        scheduleMetrics->UpdateAddMetric(transferOp);
        ASSERT_EQ(1, scheduleMetrics->operatorNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->normalOpNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->transferOpNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->operators.size());

        ASSERT_EQ(
            std::to_string(transCsInfo.GetPoolId()),
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "poolId"));
        ASSERT_EQ(
            std::to_string(transCsInfo.GetId()),
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "copySetId"));
        std::string copysetpeers = GetMetaServerHostPort(1) + "," +
                                   GetMetaServerHostPort(2) + "," +
                                   GetMetaServerHostPort(3);
        ASSERT_EQ(copysetpeers, scheduleMetrics->operators[transferOp.copysetID]
                                    .GetValueByKey("copySetPeers"));
        ASSERT_EQ(
            std::to_string(transCsInfo.GetEpoch()),
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "copySetEpoch"));
        ASSERT_EQ(
            std::to_string(transferOp.startEpoch),
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "startEpoch"));
        ASSERT_EQ(NORMAL, scheduleMetrics->operators[transferOp.copysetID]
                              .GetValueByKey("opPriority"));
        ASSERT_EQ(
            TRANSFERLEADER,
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "opType"));
        ASSERT_EQ(
            GetMetaServerHostPort(3),
            scheduleMetrics->operators[transferOp.copysetID].GetValueByKey(
                "opItem"));
        std::string res =
            std::string("{\"copySetEpoch\":\"0\",\"copySetId\":\"3\",") +
            std::string("\"copySetLeader\":") +
            std::string("\"hostname1:9000\",\"copySetPeers\":\"") +
            std::string("hostname1:9000,hostname2:9000,hostname3:9000\"") +
            std::string(",\"opItem\":\"hostname3:9000\",\"opPriority\":\"") +
            std::string("Normal\",\"opType\":\"TransferLeader\",\"poolId") +
            std::string("\":\"1\",\"startEpoch\":\"1\"}");

        ASSERT_EQ(res,
                  scheduleMetrics->operators[transferOp.copysetID].JsonBody());
        LOG(INFO)
            << "format: "
            << scheduleMetrics->operators[transferOp.copysetID].JsonBody();
    }

    {
        // 2. Remove operator from 1
        scheduleMetrics->UpdateRemoveMetric(transferOp);
        ASSERT_EQ(0, scheduleMetrics->operatorNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->transferOpNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->normalOpNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->operators.size());

        // There should be no problem removing metrics that do not exist in the
        // map
        scheduleMetrics->UpdateRemoveMetric(transferOp);
    }
}

TEST_F(ScheduleMetricsTest, test_add_rm_changeOp) {
    Operator changeOp(1, CopySetKey{1, 4}, OperatorPriority::NormalPriority,
                      steady_clock::now(), std::make_shared<ChangePeer>(1, 4));
    ::curvefs::mds::topology::CopySetInfo changeCsInfo(1, 4);
    changeCsInfo.SetCopySetMembers(std::set<MetaServerIdType>{1, 2, 3});
    changeCsInfo.SetLeader(1);

    {
        // 1. Increase operator of normal level/changePeer type
        EXPECT_CALL(*topo, GetCopySet(CopySetKey{1, 4}, _))
            .WillOnce(DoAll(SetArgPointee<1>(changeCsInfo), Return(true)));
        EXPECT_CALL(*topo, GetMetaServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetMetaServer(1)), Return(true)));
        EXPECT_CALL(*topo, GetMetaServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetMetaServer(2)), Return(true)));
        EXPECT_CALL(*topo, GetMetaServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetMetaServer(3)), Return(true)));
        EXPECT_CALL(*topo, GetMetaServer(4, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetMetaServer(4)), Return(true)));
        EXPECT_CALL(*topo, GetServer(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(1)), Return(true)));
        EXPECT_CALL(*topo, GetServer(2, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(2)), Return(true)));
        EXPECT_CALL(*topo, GetServer(3, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(3)), Return(true)));
        EXPECT_CALL(*topo, GetServer(4, _))
            .WillOnce(DoAll(SetArgPointee<1>(GetServer(4)), Return(true)));

        scheduleMetrics->UpdateAddMetric(changeOp);
        ASSERT_EQ(1, scheduleMetrics->operatorNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->normalOpNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->changeOpNum.get_value());
        ASSERT_EQ(1, scheduleMetrics->operators.size());

        ASSERT_EQ(std::to_string(changeCsInfo.GetPoolId()),
                  scheduleMetrics->operators[changeOp.copysetID].GetValueByKey(
                      "poolId"));
        ASSERT_EQ(std::to_string(changeCsInfo.GetId()),
                  scheduleMetrics->operators[changeOp.copysetID].GetValueByKey(
                      "copySetId"));
        std::string copysetpeers = GetMetaServerHostPort(1) + "," +
                                   GetMetaServerHostPort(2) + "," +
                                   GetMetaServerHostPort(3);
        ASSERT_EQ(copysetpeers,
                  scheduleMetrics->operators[changeOp.copysetID].GetValueByKey(
                      "copySetPeers"));
        ASSERT_EQ(std::to_string(changeCsInfo.GetEpoch()),
                  scheduleMetrics->operators[changeOp.copysetID].GetValueByKey(
                      "copySetEpoch"));
        ASSERT_EQ(std::to_string(changeOp.startEpoch),
                  scheduleMetrics->operators[changeOp.copysetID].GetValueByKey(
                      "startEpoch"));
        ASSERT_EQ(NORMAL,
                  scheduleMetrics->operators[changeOp.copysetID].GetValueByKey(
                      "opPriority"));
        ASSERT_EQ(CHANGEPEER,
                  scheduleMetrics->operators[changeOp.copysetID].GetValueByKey(
                      "opType"));
        ASSERT_EQ(GetMetaServerHostPort(4),
                  scheduleMetrics->operators[changeOp.copysetID].GetValueByKey(
                      "opItem"));
        std::string res =
            std::string("{\"copySetEpoch\":\"0\",\"copySetId\":\"4\",") +
            std::string("\"copySetLeader\":\"hostname1:9000\",") +
            std::string("\"copySetPeers\":\"hostname1:9000,hostname2:9000,") +
            std::string("hostname3:9000\",\"opItem\":\"hostname4:9000\",") +
            std::string("\"opPriority\":\"Normal\",\"opType\":\"ChangePeer\"") +
            std::string(",\"poolId\":\"1\",\"startEpoch\":\"1\"}");

        ASSERT_EQ(res,
                  scheduleMetrics->operators[changeOp.copysetID].JsonBody());
        LOG(INFO) << "format: "
                  << scheduleMetrics->operators[changeOp.copysetID].JsonBody();
    }

    {
        // 2. Remove operator from 1
        scheduleMetrics->UpdateRemoveMetric(changeOp);
        ASSERT_EQ(0, scheduleMetrics->operatorNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->changeOpNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->normalOpNum.get_value());
        ASSERT_EQ(0, scheduleMetrics->operators.size());

        // There should be no problem removing metrics that do not exist in the
        // map
        scheduleMetrics->UpdateRemoveMetric(changeOp);
    }
}

TEST_F(ScheduleMetricsTest, test_abnormal) {
    Operator transferOp(1, CopySetKey{1, 3}, OperatorPriority::NormalPriority,
                        steady_clock::now(),
                        std::make_shared<TransferLeader>(1, 3));
    ::curvefs::mds::topology::CopySetInfo transCsInfo(1, 3);
    transCsInfo.SetCopySetMembers(std::set<MetaServerIdType>{1, 2, 3});
    transCsInfo.SetLeader(1);

    // Failed to obtain copyset
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

    // Failed to obtain metaserver or server
    EXPECT_CALL(*topo, GetCopySet(CopySetKey{1, 3}, _))
        .WillOnce(DoAll(SetArgPointee<1>(transCsInfo), Return(true)));
    EXPECT_CALL(*topo, GetMetaServer(1, _)).WillOnce(Return(false));
    EXPECT_CALL(*topo, GetMetaServer(2, _))
        .WillOnce(DoAll(SetArgPointee<1>(GetMetaServer(2)), Return(true)));
    EXPECT_CALL(*topo, GetMetaServer(3, _))
        .Times(2)
        .WillRepeatedly(
            DoAll(SetArgPointee<1>(GetMetaServer(3)), Return(true)));
    EXPECT_CALL(*topo, GetServer(2, _)).WillOnce(Return(false));
    EXPECT_CALL(*topo, GetServer(3, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(GetServer(3)), Return(true)));
    scheduleMetrics->UpdateAddMetric(transferOp);
    std::string res =
        std::string("{\"copySetEpoch\":\"0\",\"copySetId\":\"3\",") +
        std::string("\"copySetLeader\":\"UNINITIALIZE_ID\",\"") +
        std::string("copySetPeers\":\",,hostname3:9000\",\"opItem\"") +
        std::string(":\"hostname3:9000\",\"opPriority\":\"Normal\",") +
        std::string("\"opType\":\"TransferLeader\",\"poolId\":\"1\"") +
        std::string(",\"startEpoch\":\"1\"}");

    ASSERT_EQ(res, scheduleMetrics->operators[transferOp.copysetID].JsonBody());
    LOG(INFO) << "format: "
              << scheduleMetrics->operators[transferOp.copysetID].JsonBody();
}
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
