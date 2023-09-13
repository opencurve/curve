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
#include <map>
#include <string>
#include <iterator>
#include <cmath>
#include <algorithm>
#include <random>
#include "src/mds/topology/topology_item.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_config.h"
#include "src/mds/topology/topology_service_manager.h"
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/operatorController.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/copyset/copyset_policy.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/mds/schedule/scheduleMetrics.h"
#include "test/mds/schedule/schedulerPOC/mock_topology.h"
#include "test/mds/mock/mock_topology.h"

using ::curve::mds::topology::MockTopology;

using ::curve::mds::topology::Server;
using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::CopySetInfo;
using ::curve::mds::topology::ChunkServerState;
using ::curve::mds::topology::TopologyOption;

using ::curve::mds::topology::ZoneIdType;
using ::curve::mds::topology::ServerIdType;
using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::LogicalPoolType;

using ::curve::mds::topology::ChunkServerStatus;
using ::curve::mds::topology::OnlineState;
using ::curve::mds::topology::PhysicalPoolStat;

using ::curve::mds::topology::ChunkServerFilter;
using ::curve::mds::topology::CopySetFilter;

using ::curve::mds::copyset::ChunkServerInfo;
using ::curve::mds::copyset::CopysetPolicy;
using ::curve::mds::copyset::CopysetPermutationPolicy;
using ::curve::mds::copyset::CopysetPermutationPolicyNXX;
using ::curve::mds::copyset::CopysetZoneShufflePolicy;
using ::curve::mds::copyset::Copyset;
using ::curve::mds::copyset::ClusterInfo;
using ::curve::mds::copyset::CopysetManager;
using ::curve::mds::copyset::CopysetConstrait;
namespace curve {
namespace mds {
namespace schedule {
bool leaderCountOn = false;
class FakeTopologyStat;

class FakeTopo : public ::curve::mds::topology::TopologyImpl {
 public:
    FakeTopo() : TopologyImpl(
        std::make_shared<MockIdGenerator>(),
        std::make_shared<MockTokenGenerator>(),
        std::make_shared<MockStorage>()) {}

    void BuildMassiveTopo() {
        constexpr int serverNum = 9;
        constexpr int diskNumPerServer = 20;
        constexpr int zoneNum = 3;
        constexpr int numCopysets = 6000;

        // gen server
        for (int i = 1; i <= serverNum; i++) {
            std::string internalHostIP = "10.192.0." + std::to_string(i+1);
                serverMap_[i]= Server(static_cast<ServerIdType>(i), "",
                    internalHostIP, 0, "", 0, i % zoneNum + 1, 1, "");
        }

        // gen chunkserver
        for (int i = 1; i <= serverNum; i++) {
            for (int j = 1; j <= diskNumPerServer; j++) {
                ChunkServerIdType id = j + diskNumPerServer * (i-1);
                ChunkServer chunkserver(static_cast<ChunkServerIdType>(id),
                    "", "sata", i, serverMap_[i].GetInternalHostIp(), 9000+j,
                    "", ChunkServerStatus::READWRITE);
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
            copySetMap_[info.GetCopySetKey()]  = info;
        }

        logicalPoolSet_.insert(0);
    }

    std::vector<PoolIdType> GetLogicalPoolInCluster(
        LogicalPoolFilter filter = [](const LogicalPool&) {
            return true;}) const override {
        std::vector<PoolIdType> ret;
        for (auto lid : logicalPoolSet_) {
            ret.emplace_back(lid);
        }
        return ret;
    }

    std::vector<ChunkServerIdType> GetChunkServerInCluster(
        ChunkServerFilter filter = [](const ChunkServer&) {
            return true;}) const override {
        std::vector<ChunkServerIdType> ret;
        for (auto it = chunkServerMap_.begin();
            it != chunkServerMap_.end();
            it++) {
            ret.emplace_back(it->first);
        }
        return ret;
    }

    std::list<ChunkServerIdType> GetChunkServerInLogicalPool(
        PoolIdType id,
        ChunkServerFilter filter = [](const ChunkServer&) {
            return true;}) const override {
        std::list<ChunkServerIdType> ret;
        for (auto it = chunkServerMap_.begin();
            it != chunkServerMap_.end();
            it++) {
            ret.emplace_back(it->first);
        }
        return ret;
    }

    std::list<ChunkServerIdType> GetChunkServerInServer(
        ServerIdType id,
        ChunkServerFilter filter = [](const ChunkServer&) {
            return true;}) const override {
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
            return true;}) const override {
        std::vector<CopySetKey> ret;
        for (auto it : copySetMap_) {
            ret.emplace_back(it.first);
        }
        return ret;
    }

    std::vector<CopySetKey> GetCopySetsInChunkServer(
        ChunkServerIdType csId,
        CopySetFilter filter = [](const ::curve::mds::topology::CopySetInfo&) {
            return true;}) const override {
        std::vector<CopySetKey> ret;
        for (auto it : copySetMap_) {
            if (it.second.GetCopySetMembers().count(csId) > 0) {
                ret.push_back(it.first);
            }
        }
        return ret;
    }

    std::vector<::curve::mds::topology::CopySetInfo>
    GetCopySetInfosInLogicalPool(PoolIdType logicalPoolId,
        CopySetFilter filter = [](const ::curve::mds::topology::CopySetInfo&) {
            return true;}) const override {
        std::vector<::curve::mds::topology::CopySetInfo> ret;
        for (auto it : copySetMap_) {
            if (it.first.first == logicalPoolId) {
                ret.push_back(it.second);
            }
        }

        return ret;
    }

    bool GetServer(ServerIdType serverId, Server *out) const override {
        auto it = serverMap_.find(serverId);
        if (it != serverMap_.end()) {
            *out = it->second;
            return true;
        }
        return false;
    }

    bool GetCopySet(::curve::mds::topology::CopySetKey key,
        ::curve::mds::topology::CopySetInfo *out) const override {
        auto it = copySetMap_.find(key);
        if (it != copySetMap_.end()) {
            *out = it->second;
            return true;
        } else {
            return false;
        }
    }

    bool GetChunkServer(
        ChunkServerIdType chunkserverId, ChunkServer *out) const override {
        auto it = chunkServerMap_.find(chunkserverId);
        if (it != chunkServerMap_.end()) {
            *out = it->second;
            return true;
        }
        return false;
    }

    bool GetLogicalPool(PoolIdType poolId, LogicalPool *out) const override {
        LogicalPool::RedundanceAndPlaceMentPolicy rap;
        rap.pageFileRAP.copysetNum = copySetMap_.size();
        rap.pageFileRAP.replicaNum = 3;
        rap.pageFileRAP.zoneNum = 3;

        LogicalPool pool(0, "logicalpool-0", 1, LogicalPoolType::PAGEFILE,
            rap, LogicalPool::UserPolicy{}, 0, true);
        pool.SetScatterWidth(100);
        *out = pool;
        return true;
    }

    int UpdateChunkServerOnlineState(
        const OnlineState &onlineState, ChunkServerIdType id) override {
        auto it = chunkServerMap_.find(id);
        if (it == chunkServerMap_.end()) {
            return -1;
        } else {
            it->second.SetOnlineState(onlineState);
            return 0;
        }
    }

    int UpdateChunkServerRwState(const ChunkServerStatus &rwStatus,
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
        const ::curve::mds::topology::CopySetInfo &data) override {
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
    FakeTopologyServiceManager() :
        TopologyServiceManager(std::make_shared<FakeTopo>(),
            std::static_pointer_cast<TopologyStat>(
                std::make_shared<FakeTopologyStat>(
                            std::make_shared<FakeTopo>())), nullptr,
            std::make_shared<CopysetManager>(
                ::curve::mds::copyset::CopysetOption{}), nullptr) {}

    bool CreateCopysetNodeOnChunkServer(
        ChunkServerIdType csId,
        const std::vector<::curve::mds::topology::CopySetInfo> &cs) override {
        return true;
    }
};

class FakeTopologyStat : public TopologyStat {
 public:
    explicit FakeTopologyStat(const std::shared_ptr<Topology> &topo)
        : topo_(topo) {}
    void UpdateChunkServerStat(ChunkServerIdType csId,
        const ChunkServerStat &stat) {}

    bool GetChunkServerStat(ChunkServerIdType csId, ChunkServerStat *stat) {
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

    bool GetPhysicalPoolStat(PoolIdType pId, PhysicalPoolStat* stat) override {
        stat->chunkFilePoolSize = std::numeric_limits<uint64_t>::max();
        stat->chunkFilePoolUsed = 0;
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
        // 打印初始每个chunkserver的scatter-with
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

        // 打印scatter-with的方差
        LOG(INFO) << "scatter-with (online chunkserver): " << factorMap.size();
        float avg = static_cast<float>(sumFactor) / factorMap.size();
        float variance = 0;
        for (auto it : factorMap) {
            variance += std::pow(it.second - avg, 2);
        }
        variance /= factorMap.size();
        LOG(INFO) << "###print scatter-with in online chunkserver###\n"
                  << "均值：" << avg
                  << ", 方差：" << variance
                  << ", 标准差： " << std::sqrt(variance)
                  << ", 最大值：(" << max << "," << maxId << ")"
                  << ", 最小值：(" << min << "," << minId << ")";
    }

    void PrintScatterWithInLogicalPool(PoolIdType lid = 0) {
        // 打印初始每个chunkserver的scatter-with
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

        // 打印scatter-with的方差
        LOG(INFO) << "scatter-with (all chunkserver): " << factorMap.size();
        float avg = static_cast<float>(sumFactor) / factorMap.size();
        float variance = 0;
        for (auto it : factorMap) {
            variance += std::pow(it.second - avg, 2);
        }
        variance /= factorMap.size();
        LOG(INFO) << "###print scatter-with in cluster###\n"
                  << "均值：" << avg
                  << ", 方差：" << variance
                  << ", 标准差： " << std::sqrt(variance)
                  << ", 最大值：(" << max << "," << maxId << ")"
                  << ", 最小值：(" << min << "," << minId << ")";
    }

    void PrintCopySetNumInOnlineChunkServer(PoolIdType lid = 0) {
        // 打印每个chunksever上copyset的数量
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

        // 打印方差
        float avg = static_cast<float>(sumNumber) /
            static_cast<float>(numberMap.size());
        float variance = 0;
        for (auto it : numberMap) {
            variance += std::pow(it.second - avg, 2);
        }
        variance /= numberMap.size();
        LOG(INFO) << "###print copyset-num in online chunkserver###\n"
                  << "均值：" << avg
                  << ", 方差：" << variance
                  << ", 标准差： " << std::sqrt(variance)
                  << ", 最大值：(" << max << "," << maxId << ")"
                  << "), 最小值：(" << min << "," << minId << ")";
    }

    void PrintCopySetNumInLogicalPool(PoolIdType lid = 0) {
        // 打印每个chunksever上copyset的数量
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

        // 打印方差
        float avg = static_cast<float>(sumNumber) /
            static_cast<float>(numberMap.size());
        float variance = 0;
        for (auto it : numberMap) {
            variance += std::pow(it.second - avg, 2);
        }
        variance /= numberMap.size();
        LOG(INFO) << "###print copyset-num in cluster###\n"
                  << "均值：" << avg
                  << ", 方差：" << variance
                  << ", 标准差： " << std::sqrt(variance)
                  << ", 最大值： " << max
                  << ", 最小值：" << min;
    }

    void PrintLeaderCountInChunkServer(PoolIdType lid = 0) {
        // 打印每个chunkserver上leader的数量
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
                  << "均值：" << avg
                  << ", 方差：" << variance
                  << ", 标准差： " << std::sqrt(variance)
                  << ", 最大值：(" << max << "," << maxId << ")"
                  << "), 最小值：(" << min << "," << minId << ")";
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

    // 计算每个chunkserver的scatter-with
    int GetChunkServerScatterwith(ChunkServerIdType csId) {
        // 计算chunkserver上的scatter-with
        std::map<ChunkServerIdType, int> chunkServerCount;
        for (auto it : topo_->GetCopySetsInChunkServer(csId)) {
            // get copyset info
            ::curve::mds::topology::CopySetInfo info;
            topo_->GetCopySet(it, &info);

            // 统计所分布的chunkserver
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

        // 选择[0, chunkServers.size())中的index
        std::srand(std::time(nullptr));
        int index = std::rand() % chunkServers.size();

        // 设置目标chunkserver的状态为offline
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

    void SetChunkServerOnline(const std::set<ChunkServerIdType> &list) {
        for (auto id : list) {
            SetChunkServerOnline(id);
        }
    }

    void BuildLeaderScheduler(int opConcurrent) {
        topoAdapter_ =  std::make_shared<TopoAdapterImpl>(
            topo_, std::make_shared<FakeTopologyServiceManager>(), topoStat_);

        opController_ =
            std::make_shared<OperatorController>(
                opConcurrent, std::make_shared<ScheduleMetrics>(topo_));

        leaderScheduler_ = std::make_shared<LeaderScheduler>(
            opt, topoAdapter_, opController_);
    }

    void BuildRapidLeaderScheduler(int opConcurrent) {
        topoAdapter_ =  std::make_shared<TopoAdapterImpl>(
            topo_, std::make_shared<FakeTopologyServiceManager>(), topoStat_);

        opController_ =
            std::make_shared<OperatorController>(
                opConcurrent, std::make_shared<ScheduleMetrics>(topo_));

        rapidLeaderScheduler_ = std::make_shared<RapidLeaderScheduler>(
            opt, topoAdapter_, opController_, 0);
    }

    void BuilRecoverScheduler(int opConcurrent) {
        topoAdapter_ =  std::make_shared<TopoAdapterImpl>(
            topo_, std::make_shared<FakeTopologyServiceManager>(), topoStat_);

        opController_ =
            std::make_shared<OperatorController>(
                opConcurrent, std::make_shared<ScheduleMetrics>(topo_));

        recoverScheduler_ = std::make_shared<RecoverScheduler>(
            opt, topoAdapter_, opController_);
    }

    void BuildCopySetScheduler(int opConcurrent) {
        copySetScheduler_ = std::make_shared<CopySetScheduler>(
            opt, topoAdapter_, opController_);
    }

    void ApplyOperatorsInOpController(
        const std::set<ChunkServerIdType> &list) {
        std::vector<CopySetKey> keys;
        for (auto op : opController_->GetOperators()) {
            auto type = dynamic_cast<ChangePeer *>(op.step.get());
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
            auto type = dynamic_cast<TransferLeader *>(op.step.get());
            ASSERT_TRUE(type != nullptr);

            ::curve::mds::topology::CopySetInfo info;
            ASSERT_TRUE(topo_->GetCopySet(op.copysetID, &info));
            info.SetLeader(type->GetTargetPeer());
            ASSERT_EQ(0, topo_->UpdateCopySetTopo(info));
        }
    }

    // 有两个chunkserver offline的停止条件:
    // 所有copyset均有两个及以上的副本offline
    bool SatisfyStopCondition(const std::set<ChunkServerIdType> &idList) {
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
    // 测试一个chunkserver offline恢复后的情况
    // 1. 创建recoverScheduler
    BuilRecoverScheduler(1);

    // 2. 任意选择一个chunkserver处于offline状态
    ChunkServerIdType choose = RandomOfflineOneChunkServer();

    // 3. 生成operator直到choose上没有copyset为止
    do {
        recoverScheduler_->Schedule();
        // update copyset to topology
        ApplyOperatorsInOpController(std::set<ChunkServerIdType>{choose});
    } while (topo_->GetCopySetsInChunkServer(choose).size() > 0);

    // 4. 打印最终的scatter-with
    PrintScatterWithInOnlineChunkServer();
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInOnlineChunkServer();
    PrintCopySetNumInLogicalPool();

    // =============================结果======================================
    // ===========================集群初始状态=================================
    // ###print scatter-with in cluster###
    // 均值：97.9556, 方差：11.5314, 标准差： 3.39579, 最大值：106, 最小值：88
    // ###print copyset-num in cluster###
    // 均值：100, 方差：0, 标准差： 0, 最大值： 100, 最小值：100
    // ==========================恢复之后的状态================================= //NOLINT
    // ###print scatter-with in online chunkserver###
    // 均值：均值：98.8156, 方差：10.3403, 标准差： 3.21564, 最大值：106, 最小值：95 //NOLINT
    // ###print scatter-with in cluster###
    // 均值：98.2667, 方差：64.2289, 标准差： 8.0143, 最大值：106, 最小值：0
    // ###print copyset-num in online chunkserver###
    // 均值：100.559, 方差：1.77729, 标准差： 1.33315, 最大值：109, 最小值：100
    // ###print copyset-num in cluster###
    // 均值：100, 方差：57.6333, 标准差： 7.59166, 最大值： 109, 最小值：0
}

TEST_F(CopysetSchedulerPOC, DISABLED_test_scatterwith_after_recover_2) {
    // 测试一个chunkserver offline恢复过程中，另一个chunkserver offline的情况
    // 1. 创建recoverScheduler
    BuilRecoverScheduler(1);

    // 2. 任意选择两个chunkserver处于offline状态
    std::set<ChunkServerIdType> idlist;
    ChunkServerIdType choose1 = 0;
    ChunkServerIdType choose2 = 0;
    choose1 = RandomOfflineOneChunkServer();
    idlist.emplace(choose1);

    // 3. 生成operator直到choose上没有copyset为止
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

    // 4. 打印最终的scatter-with
    PrintScatterWithInOnlineChunkServer();
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInOnlineChunkServer();
    PrintCopySetNumInLogicalPool();

    // ============================结果===================================
    // =========================集群初始状态===============================
    // ###print scatter-with in cluster###
    // 均值：97.3, 方差：9.89889, 标准差：3.14625, 最大值：106, 最小值：89
    // ###print copyset-num in cluster###
    // 均值：100, 方差：0, 标准差： 0, 最大值： 100, 最小值：100
    // =========================恢复之后的状态==============================
    // ###print scatter-with in online chunkserver###
    // 均值：100.348, 方差：7.47418, 标准差： 2.73389, 最大值：108, 最小值：101
    // ###print scatter-with in cluster###
    // 均值：99.2333, 方差：118.034, 标准差： 10.8644, 最大值：108, 最小值：0
    // ###print copyset-num in online chunkserver###
    // 均值：101.124, 方差：2.9735, 标准差： 1.72438, 最大值：112, 最小值：100
    // ###print copyset-num in cluster###
    // 均值：100, 方差：115.3, 标准差： 10.7378, 最大值： 112, 最小值：0
}

TEST_F(CopysetSchedulerPOC, DISABLED_test_scatterwith_after_recover_3) {
    // 测试一个chunkserver offline恢复过程中，接连有5个chunkserver offline
    // 1. 创建recoverScheduler
     BuilRecoverScheduler(1);

    // 2. 任意选择两个chunkserver处于offline状态
    std::set<ChunkServerIdType> idlist;
    std::vector<ChunkServerIdType> origin;
    for (int i = 0; i < 6; i++) {
        origin.emplace_back(0);
    }

    origin[0] = RandomOfflineOneChunkServer();
    idlist.emplace(origin[0]);

    // 3. 生成operator直到choose上没有copyset为止
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

    // 4. 打印最终的scatter-with
    PrintScatterWithInOnlineChunkServer();
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInOnlineChunkServer();
    PrintCopySetNumInLogicalPool();

    // ============================结果====================================
    // ========================集群初始状态=================================
    // ###print scatter-with in cluster###
    // 均值：97.6, 方差：11.8067, 标准差： 3.43608, 最大值：105, 最小值：87
    // ###print copyset-num in cluster###
    // 均值：100, 方差：0, 标准差： 0, 最大值： 100, 最小值：100
    // ========================恢复之后的状态================================
    // ###print scatter-with in online chunkserver###
    // 均值：105.425, 方差：9.95706, 标准差： 3.15548, 最大值：116, 最小值：103
    // ###print scatter-with in cluster###
    // 均值：101.933, 方差：363.262, 标准差： 19.0594, 最大值：116, 最小值：0
    // ###print copyset-num in online chunkserver###
    // 均值：103.425, 方差：13.164, 标准差： 3.62822, 最大值：121, 最小值：100
    // ###print copyset-num in cluster###
    // 均值：100, 方差：352.989, 标准差： 18.788, 最大值： 121, 最小值：0
}

TEST_F(CopysetSchedulerPOC, DISABLED_test_scatterwith_after_recover_4) {
    // 测试20个chunkserver 接连 offline
    // 1. 创建recoverScheduler
     BuilRecoverScheduler(1);

    // 2. 任意选择两个chunkserver处于offline状态
    std::set<ChunkServerIdType> idlist;
    std::vector<ChunkServerIdType> origin;
    for (int i = 0; i < 20; i++) {
        origin.emplace_back(0);
    }

    origin[0] = RandomOfflineOneChunkServer();
    idlist.emplace(origin[0]);

    // 3. 生成operator直到choose上没有copyset为止
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

    // 4. 打印最终的scatter-with
    PrintScatterWithInOnlineChunkServer();
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInOnlineChunkServer();
    PrintCopySetNumInLogicalPool();
}

TEST_F(CopysetSchedulerPOC, test_chunkserver_offline_over_concurrency) {
    // 测试一个server有多个chunkserver offline, 有一个被设置为pending,
    // 可以recover的情况
    offlineTolerent_ = 20;
    BuilRecoverScheduler(4);

    // offline一个server上的chunkserver
    auto chunkserverSet = OfflineChunkServerInServer1();
    // 选择其中一个设置为pendding状态
    ChunkServerIdType target = *chunkserverSet.begin();
    topo_->UpdateChunkServerRwState(ChunkServerStatus::PENDDING, target);

    int opNum = 0;
    int targetOpNum = topo_->GetCopySetsInChunkServer(target).size();
    // 开始恢复
    do {
        recoverScheduler_->Schedule();
        opNum += opController_->GetOperators().size();
        // apply operator, 把copyset更新到topology
        ApplyOperatorsInOpController(std::set<ChunkServerIdType>{target});
    } while (topo_->GetCopySetsInChunkServer(target).size() > 0);

    ASSERT_EQ(targetOpNum, opNum);
}

TEST_F(CopysetSchedulerPOC, test_scatterwith_after_copysetRebalance_1) { //NOLINT
    // 测试一个chunkserver offline, 集群回迁的情况

    // 1. 一个chunkserver offline后恢复
    BuilRecoverScheduler(1);
    ChunkServerIdType choose = RandomOfflineOneChunkServer();
    do {
        recoverScheduler_->Schedule();
        // apply operator, 把copyset更新到topology
        ApplyOperatorsInOpController(std::set<ChunkServerIdType>{choose});
    } while (topo_->GetCopySetsInChunkServer(choose).size() > 0);

    PrintScatterWithInOnlineChunkServer();
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInOnlineChunkServer();
    PrintCopySetNumInLogicalPool();
    // ============================结果====================================
    // ========================集群初始状态=================================
    // ###print scatter-with in cluster###
    // 均值：97.6667, 方差：10.9444, 标准差： 3.30824, 最大值：107, 最小值：90
    // ###print copyset-num in cluster###
    // 均值：100, 方差：0, 标准差： 0, 最大值： 100, 最小值：100
    // ========================恢复之后的状态================================
    // ###print scatter-with in online chunkserver###
    // 均值：99.1061, 方差：10.1172, 标准差： 3.18076, 最大值：108, 最小值：91
    // ###print scatter-with in cluster###
    // 均值：98.5556, 方差：64.3247, 标准差： 8.02027, 最大值：108, 最小值：0
    // ###print copyset-num in online chunkserver###
    // 均值：100.559, 方差：1.56499, 标准差： 1.251, 最大值：107, 最小值：100
    // ###print copyset-num in cluster###
    // 均值：100, 方差：57.4222, 标准差： 7.57774, 最大值： 107, 最小值：0

    // 2. chunkserver-choose恢复成online状态
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

    // ============================结果====================================
    // ========================迁移后的状态=================================
    // ###print scatter-with in cluster###
    // 均值：99.2667, 方差：9.65111, 标准差： 3.10662, 最大值：109, 最小值：91
    // ###print copyset-num in cluster###
    // 均值：100, 方差：0.5, 标准差： 0.707107, 最大值： 101, 最小值：91
}

TEST_F(CopysetSchedulerPOC, DISABLED_test_scatterwith_after_copysetRebalance_2) { //NOLINT
    // 测试一个chunkserver offline恢复过程中，另一个chunkserver offline
    // 集群回迁的情况

    // 1. chunkserver offline后恢复
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

    // ============================结果===================================
    // =========================集群初始状态===============================
    // ###print scatter-with in cluster###
    // 均值：97.4889, 方差：9.96099, 标准差： 3.1561, 最大值：105, 最小值：89
    // ###print copyset-num in cluster###
    // 均值：100, 方差：0, 标准差： 0, 最大值： 100, 最小值：100
    // =========================恢复之后的状态==============================
    // ###print scatter-with in online chunkserver###
    // 均值：100.472, 方差：7.37281, 标准差： 2.71529, 最大值：106, 最小值：91
    // ###print scatter-with in cluster###
    // 均值：99.3556, 方差：118.207, 标准差： 10.8723, 最大值：106, 最小值：0
    // ###print copyset-num in online chunkserver###
    // 均值：101.124, 方差：2.77125, 标准差： 1.66471, 最大值：111, 最小值：100
    // ###print copyset-num in cluster###
    // 均值：100, 方差：115.1, 标准差： 10.7285, 最大值： 111, 最小值：0

    // 2. cchunkserver恢复成online状态
    SetChunkServerOnline(choose1);
    SetChunkServerOnline(choose2);
    BuildCopySetScheduler(1);
    int removeOne = 0;
    do {
        removeOne = copySetScheduler_->Schedule();
        ApplyOperatorsInOpController(std::set<ChunkServerIdType>{removeOne});
    } while (removeOne > 0);
    PrintScatterWithInLogicalPool();
    PrintCopySetNumInLogicalPool();
    // ============================结果====================================
    // ========================迁移后的状态=================================
    // ###print scatter-with in cluster###
    // 均值：100.556, 方差：8.18025, 标准差： 2.86011, 最大值：107, 最小值：91
    // ###print copyset-num in cluster###
    // 均值：100, 方差：1, 标准差： 1, 最大值： 101, 最小值：91
}

TEST_F(CopysetSchedulerPOC, test_scatterwith_after_copysetRebalance_3) { //NOLINT
    // 测试一个chunkserver offline恢复过程中，接连有5个chunkserver offline
    // 回迁的情况

    // 1. chunkserver offline后恢复
    BuilRecoverScheduler(1);
    std::set<ChunkServerIdType> idlist;
    std::vector<ChunkServerIdType> origin;
    for (int i = 0; i < 6; i++) {
        origin.emplace_back(0);
    }

    origin[0] = RandomOfflineOneChunkServer();
    idlist.emplace(origin[0]);

    // 3. 生成operator直到choose上没有copyset为止
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

    // ============================结果====================================
    // ========================集群初始状态=================================
    // ###print scatter-with in cluster###
    // 均值：97.6, 方差：11.8067, 标准差： 3.43608, 最大值：105, 最小值：87
    // ###print copyset-num in cluster###
    // 均值：100, 方差：0, 标准差： 0, 最大值： 100, 最小值：100
    // ========================恢复之后的状态================================
    // ###print scatter-with in online chunkserver###
    // 均值：105.425, 方差：9.95706, 标准差： 3.15548, 最大值：116, 最小值：103
    // ###print scatter-with in cluster###
    // 均值：101.933, 方差：363.262, 标准差： 19.0594, 最大值：116, 最小值：0
    // ###print copyset-num in online chunkserver###
    // 均值：103.425, 方差：13.164, 标准差： 3.62822, 最大值：121, 最小值：100
    // ###print copyset-num in cluster###
    // 均值：100, 方差：352.989, 标准差： 18.788, 最大值： 121, 最小值：0

    // 2. chunkserver恢复成online状态
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

    // ============================结果====================================
    // ========================迁移后的状态=================================
    // ###print scatter-with in cluster###
    // 均值：100.556, 方差：8.18025, 标准差： 2.86011, 最大值：107, 最小值：91
    // ###print copyset-num in cluster###
    // 均值：100, 方差：1, 标准差： 1, 最大值： 101, 最小值：91
}

TEST_F(CopysetSchedulerPOC, test_scatterwith_after_copysetRebalance_4) { //NOLINT
    // set one chunkserver status from online to pendding, and the copyset on it will schedule out  //NOLINT

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
            ApplyOperatorsInOpController(
                std::set<ChunkServerIdType>{target});
        }
    } while (removeOne > 0);
    ASSERT_EQ(opNum, targetOpNum);
    ASSERT_EQ(0, topo_->GetCopySetsInChunkServer(target).size());
    PrintScatterWithInOnlineChunkServer();
    PrintCopySetNumInOnlineChunkServer();
}

TEST_F(CopysetSchedulerPOC, test_scatterwith_after_copysetRebalance_5) { //NOLINT
    // set two chunkserver status from online to pendding, and the copyset on it will schedule out  //NOLINT

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
