/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * @Date: 2022-07-28 15:52:53
 * @Author: chenwei
 */

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/schedule/operatorController.h"
#include "curvefs/src/mds/schedule/scheduleMetrics.h"
#include "curvefs/src/mds/schedule/scheduler.h"
#include "curvefs/src/mds/topology/topology_manager.h"
#include "curvefs/test/mds/mock/mock_metaserver.h"
#include "curvefs/test/mds/mock/mock_metaserver_client.h"
#include "curvefs/test/mds/mock/mock_topology.h"

namespace curvefs {
namespace mds {

using ::testing::_;
using ::testing::AnyOf;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

using curvefs::mds::MetaserverClient;
using curvefs::mds::topology::CopySetIdType;
using curvefs::mds::topology::CopySetInfo;
using curvefs::mds::topology::CopySetKey;
using curvefs::mds::topology::CreatePartitionRequest;
using curvefs::mds::topology::CreatePartitionResponse;
using curvefs::mds::topology::DefaultIdGenerator;
using curvefs::mds::topology::FsIdType;
using curvefs::mds::topology::MetaServer;
using curvefs::mds::topology::MetaServerIdType;
using curvefs::mds::topology::MetaServerSpace;
using curvefs::mds::topology::MockStorage;
using curvefs::mds::topology::MockTokenGenerator;
using curvefs::mds::topology::OnlineState;
using curvefs::mds::topology::Partition;
using curvefs::mds::topology::PartitionIdType;
using curvefs::mds::topology::Pool;
using curvefs::mds::topology::PoolIdType;
using curvefs::mds::topology::Server;
using curvefs::mds::topology::ServerIdType;
using curvefs::mds::topology::Topology;
using curvefs::mds::topology::TopologyImpl;
using curvefs::mds::topology::TopologyManager;
using curvefs::mds::topology::TopologyOption;
using curvefs::mds::topology::TopoStatusCode;
using curvefs::mds::topology::Zone;
using curvefs::mds::topology::ZoneIdType;
using curvefs::metaserver::MockMetaserverService;
using google::protobuf::util::MessageDifferencer;

using curvefs::mds::schedule::ScheduleMetrics;
using curvefs::mds::schedule::AddPeer;
using curvefs::mds::schedule::ChangePeer;
using curvefs::mds::schedule::CopySetConf;
using curvefs::mds::schedule::CopySetScheduler;
using curvefs::mds::schedule::LeaderScheduler;
using curvefs::mds::schedule::Operator;
using curvefs::mds::schedule::OperatorController;
using curvefs::mds::schedule::RecoverScheduler;
using curvefs::mds::schedule::RemovePeer;
using curvefs::mds::schedule::ScheduleOption;
using curvefs::mds::schedule::TopoAdapterImpl;
using curvefs::mds::schedule::TransferLeader;

class MetaserverBalancePOC : public ::testing::Test {
 protected:
    MetaserverBalancePOC() {}
    virtual ~MetaserverBalancePOC() {}
    virtual void SetUp() {
        std::string addr = "127.0.0.1:13146";

        idGenerator_ = std::make_shared<DefaultIdGenerator>();
        tokenGenerator_ = std::make_shared<MockTokenGenerator>();
        storage_ = std::make_shared<MockStorage>();
        topology_ = std::make_shared<TopologyImpl>(idGenerator_,
                                                   tokenGenerator_, storage_);
        TopologyOption topologyOption;
        topologyOption.createPartitionNumber = 12;
        topologyOption.maxPartitionNumberInCopyset = 20;
        topologyOption.maxCopysetNumInMetaserver = 100;

        EXPECT_CALL(*storage_, LoadClusterInfo(_)).WillRepeatedly(Return(true));
        EXPECT_CALL(*storage_, StorageClusterInfo(_))
                                .WillRepeatedly(Return(true));
        EXPECT_CALL(*storage_, LoadPool(_, _)).WillRepeatedly(Return(true));
        EXPECT_CALL(*storage_, LoadZone(_, _)).WillRepeatedly(Return(true));
        EXPECT_CALL(*storage_, LoadServer(_, _)).WillRepeatedly(Return(true));
        EXPECT_CALL(*storage_, LoadMetaServer(_, _))
                                .WillRepeatedly(Return(true));
        EXPECT_CALL(*storage_, LoadCopySet(_, _)).WillRepeatedly(Return(true));
        EXPECT_CALL(*storage_, LoadPartition(_, _))
                                .WillRepeatedly(Return(true));

        ASSERT_EQ(topology_->Init(topologyOption), TopoStatusCode::TOPO_OK);

        MetaserverOptions metaserverOptions;
        metaserverOptions.metaserverAddr = addr;
        metaserverOptions.rpcTimeoutMs = 500;
        mockMetaserverClient_ =
            std::make_shared<MockMetaserverClient>(metaserverOptions);
        serviceManager_ =
            std::make_shared<TopologyManager>(topology_, mockMetaserverClient_);
        serviceManager_->Init(topologyOption);

        metric_ = std::make_shared<ScheduleMetrics>(topology_);
        opController_ = std::make_shared<OperatorController>(2, metric_);
        topoAdapter_ =
            std::make_shared<TopoAdapterImpl>(topology_, serviceManager_);
        ScheduleOption opt;
        opt.transferLeaderTimeLimitSec = 10;
        opt.removePeerTimeLimitSec = 100;
        opt.addPeerTimeLimitSec = 1000;
        opt.changePeerTimeLimitSec = 1000;
        opt.recoverSchedulerIntervalSec = 1;
        opt.copysetSchedulerIntervalSec = 1;
        opt.balanceRatioPercent = 30;
        opt.metaserverCoolingTimeSec = 10;
        recoverScheduler_ = std::make_shared<RecoverScheduler>(
            opt, topoAdapter_, opController_);
        copysetScheduler_ = std::make_shared<CopySetScheduler>(
            opt, topoAdapter_, opController_);
    }

    virtual void TearDown() {
        idGenerator_ = nullptr;
        tokenGenerator_ = nullptr;
        storage_ = nullptr;
        topology_ = nullptr;
        mockMetaserverClient_ = nullptr;
        serviceManager_ = nullptr;
    }

 protected:
    void PrepareAddPool(PoolIdType id = 0x11,
                        const std::string &name = "testPool",
                        const Pool::RedundanceAndPlaceMentPolicy &rap =
                            Pool::RedundanceAndPlaceMentPolicy(),
                        uint64_t createTime = 0x888) {
        Pool pool(id, name, rap, createTime);
        EXPECT_CALL(*storage_, StoragePool(_)).WillOnce(Return(true));

        int ret = topology_->AddPool(pool);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
    }

    void PrepareAddZone(ZoneIdType id = 0x21,
                        const std::string &name = "testZone",
                        PoolIdType poolId = 0x11) {
        Zone zone(id, name, poolId);
        EXPECT_CALL(*storage_, StorageZone(_)).WillOnce(Return(true));

        int ret = topology_->AddZone(zone);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddPool()";
    }

    void PrepareAddServer(ServerIdType id = 0x31,
                          const std::string &hostName = "testServer",
                          PoolIdType poolId = 0x11, ZoneIdType zoneId = 0x21,
                          const std::string &internalHostIp = "testInternalIp",
                          uint32_t internalPort = 0,
                          const std::string &externalHostIp = "testExternalIp",
                          uint32_t externalPort = 0) {
        Server server(id, hostName, internalHostIp, internalPort,
                      externalHostIp, externalPort, zoneId, poolId);
        EXPECT_CALL(*storage_, StorageServer(_)).WillOnce(Return(true));

        int ret = topology_->AddServer(server);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddZone()";
    }

    void PrepareAddMetaServer(
        MetaServerIdType id = 0x41,
        const std::string &hostname = "testMetaserver",
        const std::string &token = "testToken", ServerIdType serverId = 0x31,
        OnlineState onlineState = OnlineState::ONLINE,
        const std::string &hostIp = "testInternalIp", uint32_t port = 0,
        const std::string &externalHostIp = "testExternalIp",
        uint32_t externalPort = 0) {
        MetaServer ms(id, hostname, token, serverId, hostIp, port,
                      externalHostIp, externalPort, onlineState);

        EXPECT_CALL(*storage_, StorageMetaServer(_)).WillOnce(Return(true));
        int ret = topology_->AddMetaServer(ms);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddServer()";
    }

    void PrepareTopology() {
        PoolIdType poolId = 0x11;
        ZoneIdType zoneId = 0x21;
        ServerIdType serverId = 0x31;
        MetaServerIdType msId = 0x41;
        CopySetIdType copysetId = 0x51;
        PartitionIdType partitionId = 0x61;

        Pool::RedundanceAndPlaceMentPolicy policy;
        policy.replicaNum = 3;
        policy.copysetNum = 0;
        policy.zoneNum = 3;

        PrepareAddPool(poolId, "pool1", policy);
        PrepareAddZone(zoneId, "zone1", poolId);
        PrepareAddZone(zoneId + 1, "zone2", poolId);
        PrepareAddZone(zoneId + 2, "zone3", poolId);
        PrepareAddServer(serverId, "server1", poolId, zoneId);
        PrepareAddServer(serverId + 1, "server2", poolId, zoneId + 1);
        PrepareAddServer(serverId + 2, "server3", poolId, zoneId + 2);
        for (int i = 0; i < 12; i++) {
            PrepareAddMetaServer(msId + i, "msName", "token1", 0x31 + i % 3);
            topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), msId + i);
        }

        std::map<PoolIdType, CopySetIdType> idMaxMap;
        idMaxMap[poolId] = copysetId - 1;
        idGenerator_->initCopySetIdGenerator(idMaxMap);
        idGenerator_->initPartitionIdGenerator(partitionId - 1);
    }

    void CreatePartitions() {
        FsIdType fsId = 0x01;

        for (int i = 1; i <= 12; i++) {
            CreatePartitionRequest request;
            CreatePartitionResponse response;
            request.set_fsid(fsId);
            request.set_count(i);
            serviceManager_->CreatePartitions(&request, &response);
            ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
            ASSERT_EQ(i, response.partitioninfolist().size());
            UpdateMetaServerSpace();

            for (int j = 0; j < i; j++) {
                Partition partition;
                PartitionIdType tempId =
                    response.partitioninfolist(j).partitionid();
                ASSERT_TRUE(topology_->GetPartition(tempId, &partition));
                ASSERT_EQ(response.partitioninfolist(j).copysetid(),
                          partition.GetCopySetId());
            }
        }
    }

    void UpdateMetaServerSpace() {
        std::vector<PoolIdType> poolList = topology_->GetPoolInCluster();
        for (PoolIdType poolId : poolList) {
            std::vector<CopySetInfo> copysets =
                topology_->GetCopySetInfosInPool(poolId);
            std::map<MetaServerIdType, uint32_t> copysetNumOfMetaserver;
            for (const auto &copyset : copysets) {
                std::set<MetaServerIdType> members =
                                        copyset.GetCopySetMembers();
                for (auto &it : members) {
                    copysetNumOfMetaserver[it]++;
                }
            }

            for (const auto &it : copysetNumOfMetaserver) {
                MetaServerSpace msSpace(100, it.second);
                topology_->UpdateMetaServerSpace(msSpace, it.first);
            }
        }
    }

    void PrintStatistic(PoolIdType poolId) {
        std::vector<CopySetInfo> copysets =
            topology_->GetCopySetInfosInPool(poolId);

        std::map<CopySetIdType, uint32_t> partitionNumOfCopyset;
        std::map<MetaServerIdType, uint32_t> copysetNumOfMetaserver;
        std::map<MetaServerIdType, uint32_t> partitionNumOfMetaserver;
        std::map<MetaServerIdType, uint32_t> resourceUsageOfMetaserver;
        for (const auto &copyset : copysets) {
            partitionNumOfCopyset[copyset.GetId()] = copyset.GetPartitionNum();
            std::set<MetaServerIdType> members = copyset.GetCopySetMembers();
            for (auto &it : members) {
                copysetNumOfMetaserver[it]++;
                partitionNumOfMetaserver[it] += copyset.GetPartitionNum();
            }
        }

        LOG(INFO) << "total copyset num: " << partitionNumOfCopyset.size();
        for (const auto &it : partitionNumOfCopyset) {
            LOG(INFO) << "copyset id: " << it.first
                      << ", partition num: " << it.second;
        }


        std::list<MetaServerIdType> msIds =
                        topology_->GetMetaServerInPool(poolId);
        LOG(INFO) << "total metaserver num: " << msIds.size();
        for (MetaServerIdType msId : msIds) {
            MetaServer metaserver;
            ASSERT_TRUE(topology_->GetMetaServer(msId, &metaserver));
            ASSERT_EQ(topology_->GetCopysetNumInMetaserver(msId),
                                    copysetNumOfMetaserver[msId]);
            LOG(INFO) << "metaserver id: " << msId
                      << ", copyset num: " << copysetNumOfMetaserver[msId]
                      << ", partition num: " << partitionNumOfMetaserver[msId]
                      << ", resource usage: "
                      << metaserver.GetMetaServerSpace().
                                    GetResourceUseRatioPercent();
        }
    }

    MetaServerIdType OfflineOneMetaserver(PoolIdType poolId) {
        std::list<MetaServerIdType> msIds =
            topology_->GetMetaServerInPool(poolId);
        std::srand(std::time(nullptr));
        int random_variable = std::rand();
        auto iter = msIds.begin();
        std::advance(iter, random_variable % msIds.size());
        MetaServerIdType msId = *iter;
        LOG(INFO) << "offline metaserver " << msId;

        topology_->UpdateMetaServerOnlineState(OnlineState::OFFLINE, msId);
        return msId;
    }

    void UpMetaserver(MetaServerIdType offlineMsId) {
        LOG(INFO) << "online metaerver " << offlineMsId;

        MetaServer ms;
        topology_->GetMetaServer(offlineMsId, &ms);
        MetaServerSpace space = ms.GetMetaServerSpace();
        ASSERT_EQ(0, space.GetDiskUsed());
        ASSERT_EQ(TopoStatusCode::TOPO_OK,
                  topology_->UpdateMetaServerOnlineState(OnlineState::ONLINE,
                                                         offlineMsId));
    }

    void Apply(const std::vector<Operator> &ops) {
        for (const auto &op : ops) {
            CopySetKey copysetID = op.copysetID;
            CopySetInfo topoCopyset;
            ASSERT_TRUE(topology_->GetCopySet(copysetID, &topoCopyset));

            LOG(INFO) << op.OpToString();

            // transfer leader operator
            if (dynamic_cast<TransferLeader *>(op.step.get()) != nullptr) {
                topoCopyset.SetLeader(op.step->GetTargetPeer());
            }

            // change peer operator
            ChangePeer *changePeerOp =
                dynamic_cast<ChangePeer *>(op.step.get());
            if (changePeerOp != nullptr) {
                std::set<MetaServerIdType> members =
                    topoCopyset.GetCopySetMembers();
                members.erase(changePeerOp->GetOldPeer());
                members.insert(changePeerOp->GetTargetPeer());
                topoCopyset.SetCopySetMembers(members);
                topology_->UpdateCopySetTopo(topoCopyset);

                // update target usage
                MetaServer ms;
                topology_->GetMetaServer(changePeerOp->GetTargetPeer(), &ms);
                MetaServerSpace space = ms.GetMetaServerSpace();
                space.SetDiskUsed(space.GetDiskUsed() + 1);
                topology_->UpdateMetaServerSpace(space,
                                                 changePeerOp->GetTargetPeer());

                // update source usage
                topology_->GetMetaServer(changePeerOp->GetOldPeer(), &ms);
                space = ms.GetMetaServerSpace();
                space.SetDiskUsed(space.GetDiskUsed() - 1);
                topology_->UpdateMetaServerSpace(space,
                                                 changePeerOp->GetOldPeer());
            }

            schedule::CopySetInfo copyset;
            ASSERT_TRUE(
                topoAdapter_->CopySetFromTopoToSchedule(topoCopyset, &copyset));
            CopySetConf conf;
            ASSERT_FALSE(opController_->ApplyOperator(copyset, &conf));
        }
    }

    void Schedule() {
        while (true) {
            int ret = recoverScheduler_->Schedule();
            std::vector<Operator> ops = opController_->GetOperators();
            ASSERT_EQ(ret, ops.size());
            Apply(ops);
            ASSERT_EQ(0, opController_->GetOperators().size());

            if (ret == 0) {
                break;
            }
        }
        while (true) {
            int ret = copysetScheduler_->Schedule();
            std::vector<Operator> ops = opController_->GetOperators();
            ASSERT_EQ(ret, ops.size());
            Apply(ops);
            ASSERT_EQ(0, opController_->GetOperators().size());

            if (ret == 0) {
                break;
            }
        }
    }

 protected:
    std::shared_ptr<DefaultIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
    std::shared_ptr<TopologyImpl> topology_;
    std::shared_ptr<MockMetaserverClient> mockMetaserverClient_;
    std::shared_ptr<TopologyManager> serviceManager_;

    std::shared_ptr<ScheduleMetrics> metric_;
    std::shared_ptr<TopoAdapterImpl> topoAdapter_;
    std::shared_ptr<OperatorController> opController_;
    std::shared_ptr<RecoverScheduler> recoverScheduler_;
    std::shared_ptr<CopySetScheduler> copysetScheduler_;
};

// This test run 30 rounds. In every round, it will do :
// 1. create partitions. send 12 times requests, the create partition num
//    increase from 1 to 12
// 2. offline one metaserver, schedule until no new op gerate. send 12 times
//    requests, the create partition num increase from 1 to 12
// 3. create partitions when one metaserver down,
// 4. up offline metaserver, schedule until no new op gerate
// the topo is :
//  zone1   zone2    zone3
//    ms1     ms2      ms3
//    ms4     ms5      ms6
//    ms7     ms8      ms9
//   ms10    ms11     ms12
// in every test round, it will offline one ms random.
// conf :        topologyOption.maxPartitionNumberInCopyset = 20
//               topologyOption.maxCopysetNumInMetaserver = 100;
// after test, a possible result is:
// total copyset num: 246
// copyset id: 81, partition num: 20
// copyset id: 82, partition num: 20
// copyset id: 83, partition num: 20
// copyset id: 84, partition num: 20
// copyset id: 85, partition num: 20
// copyset id: 86, partition num: 20
// copyset id: 87, partition num: 20
// copyset id: 88, partition num: 20
// copyset id: 89, partition num: 20
// copyset id: 90, partition num: 20
// copyset id: 91, partition num: 20
// copyset id: 92, partition num: 20
// copyset id: 93, partition num: 20
// copyset id: 94, partition num: 20
// copyset id: 95, partition num: 20
// copyset id: 96, partition num: 20
// copyset id: 97, partition num: 20
// copyset id: 98, partition num: 20
// copyset id: 99, partition num: 20
// copyset id: 100, partition num: 20
// copyset id: 101, partition num: 20
// copyset id: 102, partition num: 20
// copyset id: 103, partition num: 20
// copyset id: 104, partition num: 20
// copyset id: 105, partition num: 20
// copyset id: 106, partition num: 20
// copyset id: 107, partition num: 20
// copyset id: 108, partition num: 20
// copyset id: 109, partition num: 20
// copyset id: 110, partition num: 20
// copyset id: 111, partition num: 20
// copyset id: 112, partition num: 20
// copyset id: 113, partition num: 20
// copyset id: 114, partition num: 20
// copyset id: 115, partition num: 20
// copyset id: 116, partition num: 20
// copyset id: 117, partition num: 20
// copyset id: 118, partition num: 20
// copyset id: 119, partition num: 20
// copyset id: 120, partition num: 20
// copyset id: 121, partition num: 20
// copyset id: 122, partition num: 20
// copyset id: 123, partition num: 20
// copyset id: 124, partition num: 20
// copyset id: 125, partition num: 20
// copyset id: 126, partition num: 20
// copyset id: 127, partition num: 20
// copyset id: 128, partition num: 20
// copyset id: 129, partition num: 20
// copyset id: 130, partition num: 20
// copyset id: 131, partition num: 20
// copyset id: 132, partition num: 20
// copyset id: 133, partition num: 20
// copyset id: 134, partition num: 20
// copyset id: 135, partition num: 20
// copyset id: 136, partition num: 20
// copyset id: 137, partition num: 20
// copyset id: 138, partition num: 20
// copyset id: 139, partition num: 20
// copyset id: 140, partition num: 20
// copyset id: 141, partition num: 20
// copyset id: 142, partition num: 20
// copyset id: 143, partition num: 20
// copyset id: 144, partition num: 20
// copyset id: 145, partition num: 20
// copyset id: 146, partition num: 20
// copyset id: 147, partition num: 20
// copyset id: 148, partition num: 20
// copyset id: 149, partition num: 20
// copyset id: 150, partition num: 20
// copyset id: 151, partition num: 20
// copyset id: 152, partition num: 20
// copyset id: 153, partition num: 20
// copyset id: 154, partition num: 20
// copyset id: 155, partition num: 20
// copyset id: 156, partition num: 20
// copyset id: 157, partition num: 20
// copyset id: 158, partition num: 20
// copyset id: 159, partition num: 20
// copyset id: 160, partition num: 20
// copyset id: 161, partition num: 20
// copyset id: 162, partition num: 20
// copyset id: 163, partition num: 20
// copyset id: 164, partition num: 20
// copyset id: 165, partition num: 20
// copyset id: 166, partition num: 20
// copyset id: 167, partition num: 20
// copyset id: 168, partition num: 20
// copyset id: 169, partition num: 20
// copyset id: 170, partition num: 20
// copyset id: 171, partition num: 20
// copyset id: 172, partition num: 20
// copyset id: 173, partition num: 20
// copyset id: 174, partition num: 20
// copyset id: 175, partition num: 20
// copyset id: 176, partition num: 20
// copyset id: 177, partition num: 20
// copyset id: 178, partition num: 20
// copyset id: 179, partition num: 20
// copyset id: 180, partition num: 20
// copyset id: 181, partition num: 20
// copyset id: 182, partition num: 20
// copyset id: 183, partition num: 20
// copyset id: 184, partition num: 20
// copyset id: 185, partition num: 20
// copyset id: 186, partition num: 20
// copyset id: 187, partition num: 20
// copyset id: 188, partition num: 20
// copyset id: 189, partition num: 20
// copyset id: 190, partition num: 20
// copyset id: 191, partition num: 20
// copyset id: 192, partition num: 20
// copyset id: 193, partition num: 20
// copyset id: 194, partition num: 20
// copyset id: 195, partition num: 20
// copyset id: 196, partition num: 20
// copyset id: 197, partition num: 20
// copyset id: 198, partition num: 20
// copyset id: 199, partition num: 20
// copyset id: 200, partition num: 20
// copyset id: 201, partition num: 20
// copyset id: 202, partition num: 20
// copyset id: 203, partition num: 20
// copyset id: 204, partition num: 20
// copyset id: 205, partition num: 20
// copyset id: 206, partition num: 20
// copyset id: 207, partition num: 20
// copyset id: 208, partition num: 20
// copyset id: 209, partition num: 20
// copyset id: 210, partition num: 20
// copyset id: 211, partition num: 20
// copyset id: 212, partition num: 20
// copyset id: 213, partition num: 20
// copyset id: 214, partition num: 20
// copyset id: 215, partition num: 20
// copyset id: 216, partition num: 20
// copyset id: 217, partition num: 20
// copyset id: 218, partition num: 20
// copyset id: 219, partition num: 20
// copyset id: 220, partition num: 20
// copyset id: 221, partition num: 20
// copyset id: 222, partition num: 20
// copyset id: 223, partition num: 20
// copyset id: 224, partition num: 20
// copyset id: 225, partition num: 20
// copyset id: 226, partition num: 20
// copyset id: 227, partition num: 20
// copyset id: 228, partition num: 20
// copyset id: 229, partition num: 20
// copyset id: 230, partition num: 20
// copyset id: 231, partition num: 20
// copyset id: 232, partition num: 20
// copyset id: 233, partition num: 20
// copyset id: 234, partition num: 20
// copyset id: 235, partition num: 20
// copyset id: 236, partition num: 20
// copyset id: 237, partition num: 20
// copyset id: 238, partition num: 20
// copyset id: 239, partition num: 20
// copyset id: 240, partition num: 20
// copyset id: 241, partition num: 20
// copyset id: 242, partition num: 20
// copyset id: 243, partition num: 20
// copyset id: 244, partition num: 20
// copyset id: 245, partition num: 20
// copyset id: 246, partition num: 20
// copyset id: 247, partition num: 20
// copyset id: 248, partition num: 20
// copyset id: 249, partition num: 20
// copyset id: 250, partition num: 20
// copyset id: 251, partition num: 20
// copyset id: 252, partition num: 20
// copyset id: 253, partition num: 20
// copyset id: 254, partition num: 20
// copyset id: 255, partition num: 20
// copyset id: 256, partition num: 20
// copyset id: 257, partition num: 20
// copyset id: 258, partition num: 20
// copyset id: 259, partition num: 20
// copyset id: 260, partition num: 20
// copyset id: 261, partition num: 20
// copyset id: 262, partition num: 20
// copyset id: 263, partition num: 20
// copyset id: 264, partition num: 20
// copyset id: 265, partition num: 20
// copyset id: 266, partition num: 20
// copyset id: 267, partition num: 20
// copyset id: 268, partition num: 20
// copyset id: 269, partition num: 20
// copyset id: 270, partition num: 20
// copyset id: 271, partition num: 20
// copyset id: 272, partition num: 20
// copyset id: 273, partition num: 20
// copyset id: 274, partition num: 20
// copyset id: 275, partition num: 20
// copyset id: 276, partition num: 20
// copyset id: 277, partition num: 20
// copyset id: 278, partition num: 20
// copyset id: 279, partition num: 20
// copyset id: 280, partition num: 20
// copyset id: 281, partition num: 20
// copyset id: 282, partition num: 20
// copyset id: 283, partition num: 20
// copyset id: 284, partition num: 20
// copyset id: 285, partition num: 20
// copyset id: 286, partition num: 20
// copyset id: 287, partition num: 20
// copyset id: 288, partition num: 20
// copyset id: 289, partition num: 20
// copyset id: 290, partition num: 20
// copyset id: 291, partition num: 20
// copyset id: 292, partition num: 20
// copyset id: 293, partition num: 20
// copyset id: 294, partition num: 20
// copyset id: 295, partition num: 20
// copyset id: 296, partition num: 20
// copyset id: 297, partition num: 20
// copyset id: 298, partition num: 20
// copyset id: 299, partition num: 20
// copyset id: 300, partition num: 20
// copyset id: 301, partition num: 20
// copyset id: 302, partition num: 20
// copyset id: 303, partition num: 20
// copyset id: 304, partition num: 20
// copyset id: 305, partition num: 20
// copyset id: 306, partition num: 10
// copyset id: 307, partition num: 10
// copyset id: 308, partition num: 10
// copyset id: 309, partition num: 10
// copyset id: 310, partition num: 10
// copyset id: 311, partition num: 10
// copyset id: 312, partition num: 10
// copyset id: 313, partition num: 10
// copyset id: 314, partition num: 10
// copyset id: 315, partition num: 7
// copyset id: 316, partition num: 8
// copyset id: 317, partition num: 7
// copyset id: 318, partition num: 7
// copyset id: 319, partition num: 7
// copyset id: 320, partition num: 7
// copyset id: 321, partition num: 7
// copyset id: 322, partition num: 8
// copyset id: 323, partition num: 8
// copyset id: 324, partition num: 8
// copyset id: 325, partition num: 8
// copyset id: 326, partition num: 8
// total metaserver num: 12
// metaserver id: 68, copyset num: 61, partition num: 1173, resource usage: 61
// metaserver id: 65, copyset num: 61, partition num: 1152, resource usage: 61
// metaserver id: 71, copyset num: 62, partition num: 1172, resource usage: 62
// metaserver id: 74, copyset num: 62, partition num: 1183, resource usage: 62
// metaserver id: 75, copyset num: 61, partition num: 1182, resource usage: 61
// metaserver id: 66, copyset num: 61, partition num: 1152, resource usage: 61
// metaserver id: 69, copyset num: 62, partition num: 1174, resource usage: 62
// metaserver id: 72, copyset num: 62, partition num: 1172, resource usage: 62
// metaserver id: 76, copyset num: 61, partition num: 1220, resource usage: 61
// metaserver id: 67, copyset num: 61, partition num: 1142, resource usage: 61
// metaserver id: 70, copyset num: 62, partition num: 1170, resource usage: 62
// metaserver id: 73, copyset num: 62, partition num: 1148, resource usage: 62
TEST_F(MetaserverBalancePOC, test_CreatePartitionWithAvailableCopyset_Success) {
    PoolIdType poolId = 0x11;
    PrepareTopology();

    EXPECT_CALL(*mockMetaserverClient_, CreateCopySet(poolId, _, _))
        .WillRepeatedly(Return(FSStatusCode::OK));

    EXPECT_CALL(*storage_, StorageCopySet(_)).WillRepeatedly(Return(true));

    EXPECT_CALL(*storage_, StoragePartition(_)).WillRepeatedly(Return(true));
    EXPECT_CALL(*storage_, StorageClusterInfo(_)).WillRepeatedly(Return(true));
    EXPECT_CALL(*mockMetaserverClient_, CreatePartition(_, _, _, _, _, _, _))
        .WillRepeatedly(Return(FSStatusCode::OK));
    EXPECT_CALL(*mockMetaserverClient_, CreateCopySetOnOneMetaserver(_, _, _))
        .WillRepeatedly(Return(FSStatusCode::OK));

    // run 30 round
    for (int i = 0; i < 30; i++) {
        LOG(INFO) << "run test round " << i;
        // create partitions
        LOG(INFO) << "1. create partitions";
        CreatePartitions();

        // random offline one metaserver, schedule
        LOG(INFO) << "2. offline one metaserver, schedule";
        MetaServerIdType offlineMsId = OfflineOneMetaserver(poolId);
        Schedule();

        // create partitions
        LOG(INFO) << "3. create partitions when one metaserver down";
        CreatePartitions();

        // up metaserver, schedule
        LOG(INFO) << "4. up offline metaserver, schedule";
        UpMetaserver(offlineMsId);
        Schedule();
        PrintStatistic(poolId);
    }
}

}  // namespace mds
}  // namespace curvefs

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
