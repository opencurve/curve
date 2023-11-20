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
 * Created Date: Mon Apr 1th 2019
 * Author: lixiaocui
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <iterator>
#include <map>
#include <random>
#include <string>

#include "src/mds/common/mds_define.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/mds/copyset/copyset_policy.h"
#include "src/mds/schedule/operatorController.h"
#include "src/mds/schedule/scheduleMetrics.h"
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_config.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/topology/topology_service_manager.h"
#include "test/mds/mock/mock_topology.h"
#include "test/mds/schedule/schedulerPOC/mock_topology.h"

using ::curve::mds::topology::MockTopology;

using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::ChunkServerState;
using ::curve::mds::topology::CopySetInfo;
using ::curve::mds::topology::Server;
using ::curve::mds::topology::TopologyOption;

using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::LogicalPoolType;
using ::curve::mds::topology::ServerIdType;
using ::curve::mds::topology::ZoneIdType;

using ::curve::mds::topology::ChunkServerStatus;
using ::curve::mds::topology::OnlineState;

using ::curve::mds::topology::ChunkServerFilter;
using ::curve::mds::topology::CopySetFilter;

using ::curve::mds::copyset::ChunkServerInfo;
using ::curve::mds::copyset::ClusterInfo;
using ::curve::mds::copyset::Copyset;
using ::curve::mds::copyset::CopysetConstrait;
using ::curve::mds::copyset::CopysetManager;
using ::curve::mds::copyset::CopysetPermutationPolicy;
using ::curve::mds::copyset::CopysetPermutationPolicyNXX;
using ::curve::mds::copyset::CopysetPolicy;
using ::curve::mds::copyset::CopysetZoneShufflePolicy;
namespace curve {
namespace mds {
namespace schedule {
bool leaderCountOn = false;
class FakeTopologyStat;

class FakeTopo : public ::curve::mds::topology::TopologyImpl {
 public:
    FakeTopo()
        : TopologyImpl(std::make_shared<MockIdGenerator>(),
                       std::make_shared<MockTokenGenerator>(),
                       std::make_shared<MockStorage>()) {}

    void BuildMassiveTopo() {
        constexpr int serverNum = 9;
        constexpr int diskNumPerServer = 20;
        constexpr int zoneNum = 3;
        constexpr int numCopysets = 6000;

        // gen server
        for (int i = 1; i <= serverNum; i++) {
            std::string internalHostIP = "10.192.0." + std::to_string(i + 1);
            serverMap_[i] =
                Server(static_cast<ServerIdType>(i), "", internalHostIP, 0, "",
                       0, i % zoneNum + 1, 1, "");
        }

        // gen chunkserver
        for (int i = 1; i <= serverNum; i++) {
            for (int j = 1; j <= diskNumPerServer; j++) {
                ChunkServerIdType id = j + diskNumPerServer * (i - 1);
                ChunkServer chunkserver(
                    static_cast<ChunkServerIdType>(id), "", "sata", i,
                    serverMap_[i].GetInternalHostIp(), 9000 + j, "",
                    ChunkServerStatus::READWRITE);
                chunkserver.SetOnlineState(OnlineState::ONLINE);
                chunkServerMap_[id] = chunkserver;
            }
        }

        // gen copyset
        for (auto it : chunkServerMap_) {
            ::curve::mds::copyset::ChunkServerInfo info{
                it.second.GetId(),
                {serverMap_[it.second.GetServerId()].GetZoneId(), 0}};
            cluster_.AddChunkServerInfo(info);
        }

        CopysetConstrait cst;
        cst.zoneChoseNum = 3;
        cst.replicaNum = 3;
        std::vector<Copyset> copySet;
        std::shared_ptr<CopysetPermutationPolicy> permutation =
            std::make_shared<CopysetPermutationPolicyNXX>(cst);
        std::shared_ptr<CopysetPolicy> policy =
            std::make_shared<CopysetZoneShufflePolicy>(permutation);
        policy->GenCopyset(cluster_, numCopysets, &copySet);

        // build copyset map
        LOG(INFO) << "Generate " << copySet.size() << " CopySets";
        int id = 1;
        for (auto it : copySet) {
            ::curve::mds::topology::CopySetInfo info(0, id++);
            info.SetCopySetMembers(it.replicas);
            info.SetLeader(*it.replicas.begin());
            copySetMap_[info.GetCopySetKey()] = info;
        }

        logicalPoolSet_.insert(0);
    }

    std::vector<PoolIdType> GetLogicalPoolInCluster(LogicalPoolFilter filter =
                                                        [](const LogicalPool&) {
                                                            return true;
                                                        }) const override {
        std::vector<PoolIdType> ret;
        for (auto lid : logicalPoolSet_) {
            ret.emplace_back(lid);
        }
        return ret;
    }

    std::vector<ChunkServerIdType> GetChunkServerInCluster(
        ChunkServerFilter filter = [](const ChunkServer&) {
            return true;
        }) const override {
        std::vector<ChunkServerIdType> ret;
        for (auto it = chunkServerMap_.begin(); it != chunkServerMap_.end();
             it++) {
            ret.emplace_back(it->first);
        }
        return ret;
    }

    std::list<ChunkServerIdType> GetChunkServerInLogicalPool(
        PoolIdType id, ChunkServerFilter filter = [](const ChunkServer&) {
            return true;
        }) const override {
        std::list<ChunkServerIdType> ret;
        for (auto it = chunkServerMap_.begin(); it != chunkServerMap_.end();
             it++) {
            ret.emplace_back(it->first);
        }
        return ret;
    }

    std::list<ChunkServerIdType> GetChunkServerInServer(
        ServerIdType id, ChunkServerFilter filter = [](const ChunkServer&) {
            return true;
        }) const override {
        std::list<ChunkServerIdType> res;
        for (auto it : chunkServerMap_) {
            if (it.second.GetServerId() == id) {
                res.emplace_back(it.first);
            }
        }
        return res;
    }

    std::vector<CopySetKey> GetCopySetsInCluster(
        CopySetFilter filter = [](const ::curve::mds::topology::CopySetInfo&) {
            return true;
        }) const override {
        std::vector<CopySetKey> ret;
        for (auto it : copySetMap_) {
            ret.emplace_back(it.first);
        }
        return ret;
    }

    std::vector<CopySetKey> GetCopySetsInChunkServer(
        ChunkServerIdType csId,
        CopySetFilter filter = [](const ::curve::mds::topology::CopySetInfo&) {
            return true;
        }) const override {
        std::vector<CopySetKey> ret;
        for (auto it : copySetMap_) {
            if (it.second.GetCopySetMembers().count(csId) > 0) {
                ret.push_back(it.first);
            }
        }
        return ret;
    }

    std::vector<::curve::mds::topology::CopySetInfo>
    GetCopySetInfosInLogicalPool(
        PoolIdType logicalPoolId,
        CopySetFilter filter = [](const ::curve::mds::topology::CopySetInfo&) {
            return true;
        }) const override {
        std::vector<::curve::mds::topology::CopySetInfo> ret;
        for (auto it : copySetMap_) {
            if (it.first.first == logicalPoolId) {
                ret.push_back(it.second);
            }
        }

        return ret;
    }

    bool GetServer(ServerIdType serverId, Server* out) const override {
        auto it = serverMap_.find(serverId);
        if (it != serverMap_.end()) {
            *out = it->second;
            return true;
        }
        return false;
    }

    bool GetCopySet(::curve::mds::topology::CopySetKey key,
                    ::curve::mds::topology::CopySetInfo* out) const override {
        auto it = copySetMap_.find(key);
        if (it != copySetMap_.end()) {
            *out = it->second;
            return true;
        } else {
            return false;
        }
    }

    bool GetChunkServer(ChunkServerIdType chunkserverId,
                        ChunkServer* out) const override {
        auto it = chunkServerMap_.find(chunkserverId);
        if (it != chunkServerMap_.end()) {
            *out = it->second;
            return true;
        }
        return false;
    }

    bool GetLogicalPool(PoolIdType poolId, LogicalPool* out) const override {
        LogicalPool::RedundanceAndPlaceMentPolicy rap;
        rap.pageFileRAP.copysetNum = copySetMap_.size();
        rap.pageFileRAP.replicaNum = 3;
        rap.pageFileRAP.zoneNum = 3;

        LogicalPool pool(0, "logicalpool-0", 1, LogicalPoolType::PAGEFILE, rap,
                         LogicalPool::UserPolicy{}, 0, true, true);
        pool.SetScatterWidth(100);
        *out = pool;
        return true;
    }

    int UpdateChunkServerOnlineState(const OnlineState& onlineState,
                                     ChunkServerIdType id) override {
        auto it = chunkServerMap_.find(id);
        if (it == chunkServerMap_.end()) {
            return -1;
        } else {
            it->second.SetOnlineState(onlineState);
            return 0;
        }
    }

    int UpdateChunkServerRwState(const ChunkServerStatus& rwStatus,
                                 ChunkServerIdType id) {
        auto it = chunkServerMap_.find(id);
        if (it == chunkServerMap_.end()) {
            return -1;
        } else {
            it->second.SetStatus(rwStatus);
            return 0;
        }
    }

    int UpdateCopySetTopo(
        const ::curve::mds::topology::CopySetInfo& data) override {
        CopySetKey key(data.GetLogicalPoolId(), data.GetId());
        auto it = copySetMap_.find(key);
        if (it != copySetMap_.end()) {
            it->second = data;
            return 0;
        }
        LOG(ERROR) << "topo cannot find copyset(" << data.GetLogicalPoolId()
                   << "," << data.GetId() << ")";
        return -1;
    }

 private:
    std::map<ServerIdType, Server> serverMap_;
    std::map<ChunkServerIdType, ChunkServer> chunkServerMap_;
    std::map<CopySetKey, ::curve::mds::topology::CopySetInfo> copySetMap_;
    std::set<PoolIdType> logicalPoolSet_;
    ClusterInfo cluster_;
};

class FakeTopologyServiceManager : public TopologyServiceManager {
 public:
    FakeTopologyServiceManager()
        : TopologyServiceManager(std::make_shared<FakeTopo>(),
                                 std::static_pointer_cast<TopologyStat>(
                                     std::make_shared<FakeTopologyStat>(
                                         std::make_shared<FakeTopo>())),
                                 nullptr,
                                 std::make_shared<CopysetManager>(
                                     ::curve::mds::copyset::CopysetOption{}),
                                 nullptr) {}

    bool CreateCopysetNodeOnChunkServer(
        ChunkServerIdType csId,
        const std::vector<::curve::mds::topology::CopySetInfo>& cs) override {
        return true;
    }
};

class FakeTopologyStat : public TopologyStat {
 public:
    explicit FakeTopologyStat(const std::shared_ptr<Topology>& topo)
        : topo_(topo) {}
    void UpdateChunkServerStat(ChunkServerIdType csId,
                               const ChunkServerStat& stat) {}

    bool GetChunkServerStat(ChunkServerIdType csId, ChunkServerStat* stat) {
        if (!leaderCountOn) {
            stat->leaderCount = 10;
            return true;
        }
        std::vector<CopySetKey> cplist = topo_->GetCopySetsInChunkServer(csId);
        int leaderCount = 0;
        for (auto key : cplist) {
            ::curve::mds::topology::CopySetInfo out;
            if (topo_->GetCopySet(key, &out)) {
                if (out.GetLeader() == csId) {
                    leaderCount++;
                }
            }
        }
        stat->leaderCount = leaderCount;
        return true;
    }
    bool GetChunkPoolSize(PoolIdType pId, uint64_t* chunkPoolSize) {
        return true;
    }

 private:
    std::shared_ptr<Topology> topo_;
};

class CopysetSchedulerPOC : public testing::Test {
 protected:
    void SetUp() override {
        std::shared_ptr<FakeTopo> fakeTopo = std::make_shared<FakeTopo>();
        fakeTopo->BuildMassiveTopo();
        topo_ = fakeTopo;
        topoStat_ = std::make_shared<FakeTopologyStat>(topo_);
        minScatterwidth_ = 90;
        scatterwidthPercent_ = 0.2;
        copysetNumPercent_ = 0.05;
        offlineTolerent_ = 8;
        opt.leaderSchedulerIntervalSec = 1000;
        opt.recoverSchedulerIntervalSec = 1000;
        opt.copysetSchedulerIntervalSec = 1000;
        opt.transferLeaderTimeLimitSec = 10;
        opt.removePeerTimeLimitSec = 100;
        opt.addPeerTimeLimitSec = 1000;
        opt.changePeerTimeLimitSec = 1000;
        opt.recoverSchedulerIntervalSec = 1;
        opt.scatterWithRangePerent = scatterwidthPercent_;
        opt.chunkserverCoolingTimeSec = 0;
        opt.chunkserverFailureTolerance = offlineTolerent_;
        opt.copysetNumRangePercent = copysetNumPercent_;

        // leaderCountOn = true;
        PrintScatterWithInLogicalPool();
        PrintCopySetNumInLogicalPool();
        PrintLeaderCountInChunkServer();
    }

    void TearDown() override {}

    void PrintScatterWithInOnlineChunkServer(PoolIdType lid = 0) {
        // Print the initial scatter with for each chunkserver
        int sumFactor = 0;
        std::map<ChunkServerIdType, int> factorMap;
        int max = -1;
        int maxId = -1;
        int min = -1;
        int minId = -1;

        for (auto it : topo_->GetChunkServerInLogicalPool(lid)) {
            ChunkServer chunkserver;
            ASSERT_TRUE(topo_->GetChunkServer(it, &chunkserver));
            if (chunkserver.GetOnlineState() == OnlineState::OFFLINE) {
                LOG(INFO) << "chunkserver " << it << " is offline";
                continue;
            }
            if (chunkserver.GetStatus() == ChunkServerStatus::PENDDING) {
                LOG(INFO) << "chunkserver " << it << " is pendding";
                continue;
            }

            int factor = GetChunkServerScatterwith(it);
            sumFactor += factor;
            factorMap[it] = factor;
            if (max == -1 || factor > max) {
                max = factor;
                maxId = it;
            }

            if (min == -1 || factor < min) {
                min = factor;
                minId = it;
            }
            LOG(INFO) << "ONLINEPRINT chunkserverid:" << it
                      << ", scatter-with:" << factor;
        }

        // Print variance of scatter-with
        LOG(INFO) << "scatter-with (online chunkserver): " << factorMap.size();
        float avg = static_cast<float>(sumFactor) / factorMap.size();
        float variance = 0;
        for (auto it : factorMap) {
            variance += std::pow(it.second - avg, 2);
        }
        variance /= factorMap.size();
        LOG(INFO) << "###print scatter-with in online chunkserver###\n"
                  << "Mean: " << avg << ", Variance: " << variance
                  << ", Standard Deviation: " << std::sqrt(variance)
                  << ", Maximum Value: (" << max << "," << maxId << ")"
                  << ", Minimum Value: (" << min << "," << minId << ")";
    }

    void PrintScatterWithInLogicalPool(PoolIdType lid = 0) {
        // Print the initial scatter with for each chunkserver
        int sumFactor = 0;
        int max = -1;
        int maxId = -1;
        int min = -1;
        int minId = -1;
        std::map<ChunkServerIdType, int> factorMap;
        for (auto it : topo_->GetChunkServerInLogicalPool(lid)) {
            int factor = GetChunkServerScatterwith(it);
            sumFactor += factor;
            factorMap[it] = factor;
            if (max == -1 || factor > max) {
                max = factor;
                maxId = it;
            }

            if (min == -1 || factor < min) {
                min = factor;
                minId = it;
            }
            LOG(INFO) << "CLUSTERPRINT chunkserverid:" << it
                      << ", scatter-with:" << factor;
        }

        // Print variance of scatter-with
        LOG(INFO) << "scatter-with (all chunkserver): " << factorMap.size();
        float avg = static_cast<float>(sumFactor) / factorMap.size();
        float variance = 0;
        for (auto it : factorMap) {
            variance += std::pow(it.second - avg, 2);
        }
        variance /= factorMap.size();
        LOG(INFO) << "###print scatter-with in cluster###\n"
                  << "Mean: " << avg << ", Variance: " << variance
                  << ", Standard Deviation: " << std::sqrt(variance)
                  << ", Maximum Value: (" << max << "," << maxId << ")"
                  << ", Minimum Value: (" << min << "," << minId << ")";
    }

    void PrintCopySetNumInOnlineChunkServer(PoolIdType lid = 0) {
        // Print the number of copysets on each chunksever
        std::map<ChunkServerIdType, int> numberMap;
        int sumNumber = 0;
        int max = -1;
        int maxId = -1;
        int min = -1;
        int minId = -1;
        for (auto it : topo_->GetChunkServerInLogicalPool(lid)) {
            ChunkServer chunkserver;
            ASSERT_TRUE(topo_->GetChunkServer(it, &chunkserver));
            if (chunkserver.GetOnlineState() == OnlineState::OFFLINE) {
                continue;
            }
            if (chunkserver.GetStatus() == ChunkServerStatus::PENDDING) {
                continue;
            }
            int number = topo_->GetCopySetsInChunkServer(it).size();
            sumNumber += number;
            numberMap[it] = number;

            if (max == -1 || number > max) {
                max = number;
                maxId = it;
            }

            if (min == -1 || number < min) {
                min = number;
                minId = it;
            }
            LOG(INFO) << "ONLINEPRINT chunkserverid:" << it
                      << ", copyset num:" << number;
        }

        // Print Variance
        float avg = static_cast<float>(sumNumber) /
                    static_cast<float>(numberMap.size());
        float variance = 0;
        for (auto it : numberMap) {
            variance += std::pow(it.second - avg, 2);
        }
        variance /= numberMap.size();
        LOG(INFO) << "###print copyset-num in online chunkserver###\n"
                  << "Mean: " << avg << ", Variance: " << variance
                  << ", Standard Deviation: " << std::sqrt(variance)
                  << ", Maximum Value: (" << max << "," << maxId << ")"
                  << "), Minimum Value: (" << min << "," << minId << ")";
    }

    void PrintCopySetNumInLogicalPool(PoolIdType lid = 0) {
        // Print the number of copysets on each chunksever
        std::map<ChunkServerIdType, int> numberMap;
        int sumNumber = 0;
        int max = -1;
        int min = -1;
        for (auto it : topo_->GetChunkServerInLogicalPool(lid)) {
            int number = topo_->GetCopySetsInChunkServer(it).size();
            sumNumber += number;
            numberMap[it] = number;

            if (max == -1 || number > max) {
                max = number;
            }

            if (min == -1 || number < min) {
                min = number;
            }
        }

        // Print Variance
        float avg = static_cast<float>(sumNumber) /
                    static_cast<float>(numberMap.size());
        float variance = 0;
        for (auto it : numberMap) {
            variance += std::pow(it.second - avg, 2);
        }
        variance /= numberMap.size();
        LOG(INFO) << "###print copyset-num in cluster###\n"
                  << "Mean: " << avg << ", Variance: " << variance
                  << ", Standard Deviation: " << std::sqrt(variance)
                  << ", Maximum Value: " << max << ", Minimum Value: " << min;
    }

    void PrintLeaderCountInChunkServer(PoolIdType lid = 0) {
        // Print the number of leaders on each chunkserver
        std::map<ChunkServerIdType, int> leaderDistribute;
        int sumNumber = 0;
        int max = -1;
        int maxId = -1;
        int min = -1;
        int minId = -1;

        for (auto it : topo_->GetChunkServerInLogicalPool(lid)) {
            ChunkServerStat out;
            if (topoStat_->GetChunkServerStat(it, &out)) {
                leaderDistribute[it] = out.leaderCount;
                if (max == -1 || out.leaderCount > max) {
                    max = out.leaderCount;
                    maxId = it;
                }

                if (min == -1 || out.leaderCount < min) {
                    min = out.leaderCount;
                    minId = it;
                }

                sumNumber += out.leaderCount;
                LOG(INFO) << "PRINT chunkserverid:" << it
                          << ", leader num:" << out.leaderCount;
            }
        }

        float avg = static_cast<float>(sumNumber) /
                    static_cast<float>(leaderDistribute.size());
        float variance = 0;
        for (auto it : leaderDistribute) {
            variance += std::pow(it.second - avg, 2);
        }
        variance /= leaderDistribute.size();
        LOG(INFO) << "###print leader-num in cluster###\n"
                  << "Mean: " << avg << ", Variance: " << variance
                  << ", Standard Deviation: " << std::sqrt(variance)
                  << ", Maximum Value: (" << max << "," << maxId << ")"
                  << ", Minimum Value: (" << min << "," << minId << ")";
    }

    int GetLeaderCountRange(PoolIdType lid = 0) {
        int max = -1;
        int min = -1;
        for (auto it : topo_->GetChunkServerInLogicalPool(lid)) {
            ChunkServerStat out;
            if (topoStat_->GetChunkServerStat(it, &out)) {
                if (max == -1 || out.leaderCount > max) {
                    max = out.leaderCount;
                }

                if (min == -1 || out.leaderCount < min) {
                    min = out.leaderCount;
                }
            }
        }

        return max - min;
    }

    // Calculate the scatter with for each chunkserver
    int GetChunkServerScatterwith(ChunkServerIdType csId) {
        // Calculate scatter with on chunkserver
        std::map<ChunkServerIdType, int> chunkServerCount;
        for (auto it : topo_->GetCopySetsInChunkServer(csId)) {
            // get copyset info
            ::curve::mds::topology::CopySetInfo info;
            topo_->GetCopySet(it, &info);

            // Count the distributed chunkservers
            for (auto it : info.GetCopySetMembers()) {
                if (it == csId) {
                    continue;
                }

                ChunkServer chunkserver;
                topo_->GetChunkServer(it, &chunkserver);
                if (chunkserver.GetOnlineState() == OnlineState::OFFLINE) {
                    LOG(INFO) << "chunkserver " << it << "is offline";
                    continue;
                }

                if (chunkServerCount.find(it) == chunkServerCount.end()) {
                    chunkServerCount[it] = 1;
                } else {
                    chunkServerCount[it]++;
                }
            }
        }

        return chunkServerCount.size();
    }

    ChunkServerIdType RandomOfflineOneChunkServer(PoolIdType lid = 0) {
        auto chunkServers = topo_->GetChunkServerInLogicalPool(lid);

        // Select the index in [0, chunkServers.size())
        std::srand(std::time(nullptr));
        int index = std::rand() % chunkServers.size();

        // Set the status of the target chunkserver to offline
        auto it = chunkServers.begin();
        std::advance(it, index);
        topo_->UpdateChunkServerOnlineState(OnlineState::OFFLINE, *it);
        LOG(INFO) << "offline chunkserver: " << *it;
        return *it;
    }

    std::list<ChunkServerIdType> OfflineChunkServerInServer1() {
        auto chunkserverlist = topo_->GetChunkServerInServer(1);
        for (auto it : chunkserverlist) {
            topo_->UpdateChunkServerOnlineState(OnlineState::OFFLINE, it);
        }
        return chunkserverlist;
    }

    void SetChunkServerOnline(ChunkServerIdType id) {
        topo_->UpdateChunkServerOnlineState(OnlineState::ONLINE, id);
    }

    void SetChunkServerOnline(const std::set<ChunkServerIdType>& list) {
        for (auto id : list) {
            SetChunkServerOnline(id);
        }
    }

    void BuildLeaderScheduler(int opConcurrent) {
        topoAdapter_ = std::make_shared<TopoAdapterImpl>(
            topo_, std::make_shared<FakeTopologyServiceManager>(), topoStat_);

        opController_ = std::make_shared<OperatorController>(
            opConcurrent, std::make_shared<ScheduleMetrics>(topo_));

        leaderScheduler_ =
            std::make_shared<LeaderScheduler>(opt, topoAdapter_, opController_);
    }

    void BuildRapidLeaderScheduler(int opConcurrent) {
        topoAdapter_ = std::make_shared<TopoAdapterImpl>(
            topo_, std::make_shared<FakeTopologyServiceManager>(), topoStat_);

        opController_ = std::make_shared<OperatorController>(
            opConcurrent, std::make_shared<ScheduleMetrics>(topo_));

        rapidLeaderScheduler_ = std::make_shared<RapidLeaderScheduler>(
            opt, topoAdapter_, opController_, 0);
    }

    void BuilRecoverScheduler(int opConcurrent) {
        topoAdapter_ = std::make_shared<TopoAdapterImpl>(
            topo_, std::make_shared<FakeTopologyServiceManager>(), topoStat_);

        opController_ = std::make_shared<OperatorController>(
            opConcurrent, std::make_shared<ScheduleMetrics>(topo_));

        recoverScheduler_ = std::make_shared<RecoverScheduler>(
            opt, topoAdapter_, opController_);
    }

    void BuildCopySetScheduler(int opConcurrent) {
        copySetScheduler_ = std::make_shared<CopySetScheduler>(
            opt, topoAdapter_, opController_);
    }

    void ApplyOperatorsInOpController(const std::set<ChunkServerIdType>& list) {
        std::vector<CopySetKey> keys;
        for (auto op : opController_->GetOperators()) {
            auto type = dynamic_cast<ChangePeer*>(op.step.get());
            ASSERT_TRUE(type != nullptr);
            ASSERT_TRUE(list.end() != list.find(type->GetOldPeer()));

            ::curve::mds::topology::CopySetInfo info;
            ASSERT_TRUE(topo_->GetCopySet(op.copysetID, &info));
            auto members = info.GetCopySetMembers();
            auto it = members.find(type->GetOldPeer());
            if (it == members.end()) {
                continue;
            }

            members.erase(it);
            members.emplace(type->GetTargetPeer());

            info.SetCopySetMembers(members);
            ASSERT_EQ(0, topo_->UpdateCopySetTopo(info));

            keys.emplace_back(op.copysetID);
        }
        for (auto key : keys) {
            opController_->RemoveOperator(key);
        }
    }

    void ApplyTranferLeaderOperator() {
        for (auto op : opController_->GetOperators()) {
            auto type = dynamic_cast<TransferLeader*>(op.step.get());
            ASSERT_TRUE(type != nullptr);

            ::curve::mds::topology::CopySetInfo info;
            ASSERT_TRUE(topo_->GetCopySet(op.copysetID, &info));
            info.SetLeader(type->GetTargetPeer());
            ASSERT_EQ(0, topo_->UpdateCopySetTopo(info));
        }
    }

    // There are two stopping conditions for chunkserver offline:
    // All copysets have two or more copies offline
    bool SatisfyStopCondition(const std::set<ChunkServerIdType>& idList) {
        std::vector<::curve::mds::topology::CopySetKey> copysetList;
        for (auto id : idList) {
            auto list = topo_->GetCopySetsInChunkServer(id);
            copysetList.insert(copysetList.end(), list.begin(), list.end());
        }

        if (copysetList.empty()) {
            return true;
        }

        for (auto item : copysetList) {
            ::curve::mds::topology::CopySetInfo info;
            topo_->GetCopySet(item, &info);

            int offlineNum = 0;
            for (auto member : info.GetCopySetMembers()) {
                ::curve::mds::topology::ChunkServer chunkserver;
                topo_->GetChunkServer(member, &chunkserver);
                if (chunkserver.GetOnlineState() == OnlineState::OFFLINE) {
                    offlineNum++;
                }
            }
            if (offlineNum <= 1) {
                return false;
            }
        }
        return true;
    }

 protected:
    std::shared_ptr<Topology> topo_;
    std::shared_ptr<TopologyStat> topoStat_;
    std::shared_ptr<OperatorController> opController_;
    std::shared_ptr<TopoAdapterImpl> topoAdapter_;
    std::shared_ptr<RecoverScheduler> recoverScheduler_;
    std::shared_ptr<CopySetScheduler> copySetScheduler_;
    std::shared_ptr<LeaderScheduler> leaderScheduler_;
    std::shared_ptr<RapidLeaderScheduler> rapidLeaderScheduler_;

    int minScatterwidth_;
    float scatterwidthPercent_;
    float copysetNumPercent_;
    int offlineTolerent_;
    ScheduleOption opt;
};

TEST_F(CopysetSchedulerPOC, DISABLED_test_scatterwith_after_recover_1) {
    // Testing the situation of a chunkserver offline recovery
    // 1. Create recoverScheduler
    BuilRecoverScheduler(1);

    // 2. Select any chunkserver to be offline
    ChunkServerIdType choose = RandomOfflineOneChunkServer();

    // 3. Generate operator until there is no copyset on choose
    do {
        recoverScheduler_->Schedule();
        // update copyset to topology
        ApplyOperatorsInOpController(std::set<ChunkServerIdType>{choose});
    } while (topo_->GetCopySetsInChunkServer(choose).size() > 0);

    // 4. Print the final scatter with
    PrintScatterWithInOnlineChunkServer();
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInOnlineChunkServer();
    PrintCopySetNumInLogicalPool();

    // =============================Result======================================
    // =============================Initial state of the
    // cluster=============================
    // ###print scatter-with in cluster###
    // Mean: 97.9556, Variance: 11.5314, Standard Deviation: 3.39579, Max: 106,
    // Min: 88
    // ###print copyset-num in cluster###
    // Mean: 100, Variance: 0, Standard Deviation: 0, Max: 100, Min: 100
    // =============================Status after
    // Recovery=================================
    // //NOLINT
    // ###print scatter-with in online chunkserver###
    // Mean: 98.8156, variance: 10.3403, standard deviation: 3.21564, maximum
    // value: 106, Minimum value: 95//NOLINT
    // ###print scatter-with in cluster###
    // Mean: 98.2667, Variance: 64.2289, Standard Deviation: 8.0143, Max: 106,
    // Min: 0
    // ###print copyset-num in online chunkserver###
    // Mean value: 100.559, variance: 1.77729, standard deviation: 1.33315,
    // maximum value: 109, minimum value: 100
    // ###print copyset-num in cluster###
    // Mean value: 100, variance: 57.6333, standard deviation: 7.59166, maximum
    // value: 109, minimum value: 0
}

TEST_F(CopysetSchedulerPOC, DISABLED_test_scatterwith_after_recover_2) {
    // Testing the situation of another chunkserver offline during the recovery
    // process of one chunkserver offline
    // 1. Create recoverScheduler
    BuilRecoverScheduler(1);

    // 2. Choose any two chunkservers to be offline
    std::set<ChunkServerIdType> idlist;
    ChunkServerIdType choose1 = 0;
    ChunkServerIdType choose2 = 0;
    choose1 = RandomOfflineOneChunkServer();
    idlist.emplace(choose1);

    // 3. Generate operator until there is no copyset on choose
    do {
        recoverScheduler_->Schedule();

        if (choose2 == 0) {
            choose2 = RandomOfflineOneChunkServer();
            idlist.emplace(choose2);
        }

        // update copyset to topology
        ApplyOperatorsInOpController(std::set<ChunkServerIdType>{choose1});
        ApplyOperatorsInOpController(std::set<ChunkServerIdType>{choose2});
    } while (!SatisfyStopCondition(idlist));

    // 4. Print the final scatter-with
    PrintScatterWithInOnlineChunkServer();
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInOnlineChunkServer();
    PrintCopySetNumInLogicalPool();

    // ===================================Result===================================
    // ===================================Initial state of the
    // cluster===============================
    // ###print scatter-with in cluster###
    // Mean value: 97.3, variance: 9.89889, standard deviation: 3.14625, maximum
    // value: 106, minimum value: 89
    // ###print copyset-num in cluster###
    // Mean: 100, Variance: 0, Standard Deviation: 0, Max: 100, Min: 100
    // ===================================Status after
    // Recovery==============================
    // ###print scatter-with in online chunkserver###
    // Mean value: 100.348, variance: 7.47418, standard deviation: 2.73389,
    // maximum value: 108, minimum value: 101
    // ###print scatter-with in cluster###
    // Mean value: 99.2333, variance: 118.034, standard deviation: 10.8644,
    // maximum value: 108, minimum value: 0
    // ###print copyset-num in online chunkserver###
    // Mean value: 101.124, variance: 2.9735, standard deviation: 1.72438,
    // maximum value: 112, minimum value: 100
    // ###print copyset-num in cluster###
    // Mean value: 100, variance: 115.3, standard deviation: 10.7378, maximum
    // value: 112, minimum value: 0
}

TEST_F(CopysetSchedulerPOC, DISABLED_test_scatterwith_after_recover_3) {
    // During the recovery process of testing a chunkserver offline, there were
    // 5 consecutive chunkserver offline
    // 1. Create recoverScheduler
    BuilRecoverScheduler(1);

    // 2. Choose any two chunkservers to be offline
    std::set<ChunkServerIdType> idlist;
    std::vector<ChunkServerIdType> origin;
    for (int i = 0; i < 6; i++) {
        origin.emplace_back(0);
    }

    origin[0] = RandomOfflineOneChunkServer();
    idlist.emplace(origin[0]);

    // 3. Generate operator until there is no copyset on choose
    do {
        recoverScheduler_->Schedule();

        for (int i = 1; i < 6; i++) {
            if (origin[i] == 0) {
                origin[i] = RandomOfflineOneChunkServer();
                idlist.emplace(origin[i]);
                ApplyOperatorsInOpController(idlist);
                break;
            }
        }

        ApplyOperatorsInOpController(idlist);
    } while (!SatisfyStopCondition(idlist));

    // 4. Print the final scatter with
    PrintScatterWithInOnlineChunkServer();
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInOnlineChunkServer();
    PrintCopySetNumInLogicalPool();

    // ====================================Result====================================
    // ====================================Initial state of the
    // cluster=================================
    // ###print scatter-with in cluster###
    // Mean value: 97.6, variance: 11.8067, standard deviation: 3.43608, maximum
    // value: 105, minimum value: 87
    // ###print copyset-num in cluster###
    // Mean: 100, Variance: 0, Standard Deviation: 0, Max: 100, Min: 100
    // ====================================Status after
    // Recovery================================
    // ###print scatter-with in online chunkserver###
    // Mean value: 105.425, variance: 9.95706, standard deviation: 3.15548,
    // maximum value: 116, minimum value: 103
    // ###print scatter-with in cluster###
    // Mean value: 101.933, variance: 363.262, standard deviation: 19.0594,
    // maximum value: 116, minimum value: 0
    // ###print copyset-num in online chunkserver###
    // Mean value: 103.425, variance: 13.164, standard deviation: 3.62822,
    // maximum value: 121, minimum value: 100
    // ###print copyset-num in cluster###
    // Mean value: 100, variance: 352.989, standard deviation: 18.788, maximum
    // value: 121, minimum value: 0
}

TEST_F(CopysetSchedulerPOC, DISABLED_test_scatterwith_after_recover_4) {
    // Test 20 chunkservers connected offline
    // 1. Create recoverScheduler
    BuilRecoverScheduler(1);

    // 2. Choose any two chunkservers to be offline
    std::set<ChunkServerIdType> idlist;
    std::vector<ChunkServerIdType> origin;
    for (int i = 0; i < 20; i++) {
        origin.emplace_back(0);
    }

    origin[0] = RandomOfflineOneChunkServer();
    idlist.emplace(origin[0]);

    // 3. Generate operator until there is no copyset on choose
    do {
        recoverScheduler_->Schedule();

        for (int i = 1; i < 20; i++) {
            if (origin[i] == 0) {
                origin[i] = RandomOfflineOneChunkServer();
                idlist.emplace(origin[i]);
                ApplyOperatorsInOpController(idlist);
                break;
            }
        }

        ApplyOperatorsInOpController(idlist);
    } while (!SatisfyStopCondition(idlist));

    // 4. Print the final scatter-with
    PrintScatterWithInOnlineChunkServer();
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInOnlineChunkServer();
    PrintCopySetNumInLogicalPool();
}

TEST_F(CopysetSchedulerPOC, test_chunkserver_offline_over_concurrency) {
    // Testing a server with multiple chunkservers offline, with one set to
    // pending, Conditions that can be recovered
    offlineTolerent_ = 20;
    BuilRecoverScheduler(4);

    // Offline Chunkserver on a server
    auto chunkserverSet = OfflineChunkServerInServer1();
    // Select one of the settings as pending status
    ChunkServerIdType target = *chunkserverSet.begin();
    topo_->UpdateChunkServerRwState(ChunkServerStatus::PENDDING, target);

    int opNum = 0;
    int targetOpNum = topo_->GetCopySetsInChunkServer(target).size();
    // Start recovery
    do {
        recoverScheduler_->Schedule();
        opNum += opController_->GetOperators().size();
        // Apply operator, update copyset to topology
        ApplyOperatorsInOpController(std::set<ChunkServerIdType>{target});
    } while (topo_->GetCopySetsInChunkServer(target).size() > 0);

    ASSERT_EQ(targetOpNum, opNum);
}

TEST_F(CopysetSchedulerPOC,
       test_scatterwith_after_copysetRebalance_1) {  // NOLINT
    // Testing a cluster offline and cluster fetch situation

    // 1. Restore after a chunkserver is offline
    BuilRecoverScheduler(1);
    ChunkServerIdType choose = RandomOfflineOneChunkServer();
    do {
        recoverScheduler_->Schedule();
        // Apply operator, update copyset to topology
        ApplyOperatorsInOpController(std::set<ChunkServerIdType>{choose});
    } while (topo_->GetCopySetsInChunkServer(choose).size() > 0);

    PrintScatterWithInOnlineChunkServer();
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInOnlineChunkServer();
    PrintCopySetNumInLogicalPool();
    // ====================================Result====================================
    // ====================================Initial state of the
    // cluster=================================
    // ###print scatter-with in cluster###
    // Mean value: 97.6667, variance: 10.9444, standard deviation: 3.30824,
    // maximum value: 107, minimum value: 90
    // ###print copyset-num in cluster###
    // Mean: 100, Variance: 0, Standard Deviation: 0, Max: 100, Min: 100
    // ====================================Status after
    // Recovery================================
    // ###print scatter-with in online chunkserver###
    // Mean value: 99.1061, variance: 10.1172, standard deviation: 3.18076,
    // maximum value: 108, minimum value: 91
    // ###print scatter-with in cluster###
    // Mean value: 98.5556, variance: 64.3247, standard deviation: 8.02027,
    // maximum value: 108, minimum value: 0
    // ###print copyset-num in online chunkserver###
    // Mean value: 100.559, variance: 1.56499, standard deviation: 1.251,
    // maximum value: 107, minimum value: 100
    // ###print copyset-num in cluster###
    // Mean value: 100, variance: 57.4222, standard deviation: 7.57774, maximum
    // value: 107, minimum value: 0

    // 2. Chunkserver house restore to online state
    SetChunkServerOnline(choose);
    BuildCopySetScheduler(1);
    std::vector<ChunkServerIdType> csList;
    csList = topo_->GetChunkServerInCluster();
    std::set<ChunkServerIdType> csSet(csList.begin(), csList.end());
    int operatorCount = 0;
    do {
        operatorCount = copySetScheduler_->Schedule();
        ApplyOperatorsInOpController(csSet);
    } while (operatorCount > 0);
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInLogicalPool();
    LOG(INFO) << "offline one:" << choose;
    ASSERT_TRUE(GetChunkServerScatterwith(choose) <=
                minScatterwidth_ * (1 + scatterwidthPercent_));
    ASSERT_TRUE(GetChunkServerScatterwith(choose) >= minScatterwidth_);

    // ====================================Result====================================
    // ====================================Status after
    // Migration=================================
    // ###print scatter-with in cluster###
    // Mean value: 99.2667, variance: 9.65111, standard deviation: 3.10662,
    // maximum value: 109, minimum value: 91
    // ###print copyset-num in cluster###
    // Mean value: 100, variance: 0.5, standard deviation: 0.707107, maximum
    // value: 101, minimum value: 91
}

TEST_F(CopysetSchedulerPOC,
       DISABLED_test_scatterwith_after_copysetRebalance_2) {  // NOLINT
    // During the recovery process of testing one chunkserver offline, another
    // chunkserver offline Cluster fetch situation

    // 1. Restore after chunkserver offline
    BuilRecoverScheduler(1);
    std::set<ChunkServerIdType> idlist;
    ChunkServerIdType choose1 = 0;
    ChunkServerIdType choose2 = 0;
    choose1 = RandomOfflineOneChunkServer();
    idlist.emplace(choose1);
    do {
        recoverScheduler_->Schedule();

        if (choose2 == 0) {
            choose2 = RandomOfflineOneChunkServer();
            idlist.emplace(choose2);
        }

        // update copyset to topology
        ApplyOperatorsInOpController(std::set<ChunkServerIdType>{choose1});
        ApplyOperatorsInOpController(std::set<ChunkServerIdType>{choose2});
    } while (!SatisfyStopCondition(idlist));
    PrintScatterWithInOnlineChunkServer();
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInOnlineChunkServer();
    PrintCopySetNumInLogicalPool();

    // ===================================Result===================================
    // ===================================Initial state of the
    // cluster===============================
    // ###print scatter-with in cluster###
    // Mean value: 97.4889, variance: 9.96099, standard deviation: 3.1561,
    // maximum value: 105, minimum value: 89
    // ###print copyset-num in cluster###
    // Mean: 100, Variance: 0, Standard Deviation: 0, Max: 100, Min: 100
    // ===================================Status after
    // Recovery==============================
    // ###print scatter-with in online chunkserver###
    // Mean value: 100.472, variance: 7.37281, standard deviation: 2.71529,
    // maximum value: 106, minimum value: 91
    // ###print scatter-with in cluster###
    // Mean value: 99.3556, variance: 118.207, standard deviation: 10.8723,
    // maximum value: 106, minimum value: 0
    // ###print copyset-num in online chunkserver###
    // Mean value: 101.124, variance: 2.77125, standard deviation: 1.66471,
    // maximum value: 111, minimum value: 100
    // ###print copyset-num in cluster###
    // Mean value: 100, variance: 115.1, standard deviation: 10.7285, maximum
    // value: 111, minimum value: 0

    // 2. Restore cchunkserver to online state
    SetChunkServerOnline(choose1);
    SetChunkServerOnline(choose2);
    BuildCopySetScheduler(1);
    int removeOne = 0;
    do {
        removeOne = copySetScheduler_->Schedule();
        ApplyOperatorsInOpController(std::set<ChunkServerIdType>{
            static_cast<ChunkServerIdType>(removeOne)});
    } while (removeOne > 0);
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInLogicalPool();
    // ===================================Result====================================
    // ===================================Status after
    // Migration=================================
    // ###print scatter-with in cluster###
    // Mean value: 100.556, variance: 8.18025, standard deviation: 2.86011,
    // maximum value: 107, minimum value: 91
    // ###print copyset-num in cluster###
    // Mean: 100, Variance: 1, Standard Deviation: 1, Maximum: 101, Minimum: 91
}

TEST_F(CopysetSchedulerPOC,
       test_scatterwith_after_copysetRebalance_3) {  // NOLINT
    // During the recovery process of testing a chunkserver offline, there were
    // 5 consecutive chunkserver offline Migration situation

    // 1. Restore after chunkserver offline
    BuilRecoverScheduler(1);
    std::set<ChunkServerIdType> idlist;
    std::vector<ChunkServerIdType> origin;
    for (int i = 0; i < 6; i++) {
        origin.emplace_back(0);
    }

    origin[0] = RandomOfflineOneChunkServer();
    idlist.emplace(origin[0]);

    // 3. Generate operator until there is no copyset on choose
    do {
        recoverScheduler_->Schedule();

        for (int i = 1; i < 6; i++) {
            if (origin[i] == 0) {
                origin[i] = RandomOfflineOneChunkServer();
                idlist.emplace(origin[i]);
                ApplyOperatorsInOpController(
                    std::set<ChunkServerIdType>{idlist});
                break;
            }
        }

        ApplyOperatorsInOpController(idlist);
    } while (!SatisfyStopCondition(idlist));
    PrintScatterWithInOnlineChunkServer();
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInOnlineChunkServer();
    PrintCopySetNumInLogicalPool();

    // ===================================Result====================================
    // ===================================Initial state of the
    // cluster=================================
    // ###print scatter-with in cluster###
    // Mean value: 97.6, variance: 11.8067, standard deviation: 3.43608, maximum
    // value: 105, minimum value: 87
    // ###print copyset-num in cluster###
    // Mean: 100, Variance: 0, Standard Deviation: 0, Max: 100, Min: 100
    // ===================================Status after
    // Recovery================================
    // ###print scatter-with in online chunkserver###
    // Mean value: 105.425, variance: 9.95706, standard deviation: 3.15548,
    // maximum value: 116, minimum value: 103
    // ###print scatter-with in cluster###
    // Mean value: 101.933, variance: 363.262, standard deviation: 19.0594,
    // maximum value: 116, minimum value: 0
    // ###print copyset-num in online chunkserver###
    // Mean value: 103.425, variance: 13.164, standard deviation: 3.62822,
    // maximum value: 121, minimum value: 100
    // ###print copyset-num in cluster###
    // Mean value: 100, variance: 352.989, standard deviation: 18.788, maximum
    // value: 121, minimum value: 0

    // 2. Chunkserver restored to online state
    SetChunkServerOnline(idlist);
    BuildCopySetScheduler(1);
    std::vector<ChunkServerIdType> csList;
    csList = topo_->GetChunkServerInCluster();
    std::set<ChunkServerIdType> csSet(csList.begin(), csList.end());
    int operatorCount = 0;
    do {
        operatorCount = copySetScheduler_->Schedule();
        if (operatorCount > 0) {
            ApplyOperatorsInOpController(csSet);
        }
    } while (operatorCount > 0);
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInLogicalPool();

    for (auto choose : idlist) {
        ASSERT_TRUE(GetChunkServerScatterwith(choose) <=
                    minScatterwidth_ * (1 + scatterwidthPercent_));
        ASSERT_TRUE(GetChunkServerScatterwith(choose) >= minScatterwidth_);
    }

    // ===================================Result====================================
    // ===================================Status after
    // Migration=================================
    // ###print scatter-with in cluster###
    // Mean value: 100.556, variance: 8.18025, standard deviation: 2.86011,
    // maximum value: 107, minimum value: 91
    // ###print copyset-num in cluster###
    // Mean: 100, Variance: 1, Standard Deviation: 1, Maximum: 101, Minimum: 91
}

TEST_F(CopysetSchedulerPOC,
       test_scatterwith_after_copysetRebalance_4) {  // NOLINT
    // set one chunkserver status from online to pendding, and the copyset on it
    // will schedule out  //NOLINT

    // set one chunkserver status to pendding
    auto chunkserverlist = topo_->GetChunkServerInServer(1);
    ChunkServerIdType target = *chunkserverlist.begin();
    topo_->UpdateChunkServerRwState(ChunkServerStatus::PENDDING, target);

    int opNum = 0;
    int targetOpNum = topo_->GetCopySetsInChunkServer(target).size();
    BuilRecoverScheduler(1);
    BuildCopySetScheduler(1);
    int removeOne = 0;
    do {
        removeOne = copySetScheduler_->Schedule();
        opNum += removeOne;
        if (removeOne > 0) {
            ApplyOperatorsInOpController(std::set<ChunkServerIdType>{target});
        }
    } while (removeOne > 0);
    ASSERT_EQ(opNum, targetOpNum);
    ASSERT_EQ(0, topo_->GetCopySetsInChunkServer(target).size());
    PrintScatterWithInOnlineChunkServer();
    PrintCopySetNumInOnlineChunkServer();
}

TEST_F(CopysetSchedulerPOC,
       test_scatterwith_after_copysetRebalance_5) {  // NOLINT
    // set two chunkserver status from online to pendding, and the copyset on it
    // will schedule out  //NOLINT

    // set two chunkserver status to pendding
    auto chunkserverlist = topo_->GetChunkServerInServer(1);
    ChunkServerIdType target = *chunkserverlist.begin();
    topo_->UpdateChunkServerRwState(ChunkServerStatus::PENDDING, target);
    topo_->UpdateChunkServerRwState(ChunkServerStatus::PENDDING, target + 1);

    int opNum = 0;
    int targetOpNum = topo_->GetCopySetsInChunkServer(target).size();
    targetOpNum += topo_->GetCopySetsInChunkServer(target + 1).size();
    BuilRecoverScheduler(1);
    BuildCopySetScheduler(1);
    int removeCount = 0;
    do {
        removeCount = copySetScheduler_->Schedule();
        opNum += removeCount;
        if (removeCount > 0) {
            LOG(INFO) << "removeCount = " << removeCount;
            ApplyOperatorsInOpController(
                std::set<ChunkServerIdType>{target, target + 1});
        }
    } while (removeCount > 0);
    ASSERT_EQ(opNum, targetOpNum);
    ASSERT_EQ(0, topo_->GetCopySetsInChunkServer(target).size());
    PrintScatterWithInOnlineChunkServer();
    PrintCopySetNumInOnlineChunkServer();
}

TEST_F(CopysetSchedulerPOC, DISABLED_test_leader_rebalance) {
    leaderCountOn = true;
    BuildLeaderScheduler(4);

    int opnum = 0;
    do {
        opnum = leaderScheduler_->Schedule();
        if (opnum > 0) {
            ApplyTranferLeaderOperator();
        }
    } while (opnum > 0);

    PrintLeaderCountInChunkServer();
    leaderCountOn = false;
}

TEST_F(CopysetSchedulerPOC, test_rapidleader_rebalance) {
    leaderCountOn = true;
    BuildRapidLeaderScheduler(1);

    ASSERT_EQ(kScheduleErrCodeSuccess, rapidLeaderScheduler_->Schedule());
    ApplyTranferLeaderOperator();
    ASSERT_EQ(kScheduleErrCodeSuccess, rapidLeaderScheduler_->Schedule());
    ApplyTranferLeaderOperator();

    PrintLeaderCountInChunkServer();
    ASSERT_LE(GetLeaderCountRange(), 5);
    leaderCountOn = false;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
