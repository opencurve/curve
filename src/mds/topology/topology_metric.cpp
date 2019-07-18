/*
 * Project: curve
 * Created Date: Thu Jun 27 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include "src/mds/topology/topology_metric.h"

#include <set>
#include <memory>

#include "src/mds/copyset/copyset_validation.h"

using ::curve::mds::copyset::StatisticsTools;

namespace curve {
namespace mds {
namespace topology {

std::map<PoolIdType, LogicalPoolMetricPtr> gLogicalPoolMetrics;
std::map<ChunkServerIdType, ChunkServerMetricPtr> gChunkServerMetrics;

void TopologyMetricService::UpdateTopologyMetrics() {
    // 处理chunkserver
    std::vector<ChunkServerIdType> chunkservers =
        topo_->GetChunkServerInCluster(
                [](const ChunkServer &cs) {
                    return cs.GetStatus() != ChunkServerStatus::RETIRED;
                });

    for (auto csId : chunkservers) {
        auto it = gChunkServerMetrics.find(csId);
        if (it == gChunkServerMetrics.end()) {
            ChunkServerMetricPtr cptr(
                new ChunkServerMetric(csId));
            it = gChunkServerMetrics.emplace(
                csId, std::move(cptr)).first;
        }
        ChunkServer cs;
        if (topo_->GetChunkServer(csId, &cs)) {
            it->second->diskCapacity.set_value(
                cs.GetChunkServerState().GetDiskCapacity());
            it->second->diskUsed.set_value(
                cs.GetChunkServerState().GetDiskUsed());
        }
        ChunkServerStat csStat;
        if (topoStat_->GetChunkServerStat(csId, &csStat)) {
            it->second->leaderCount.set_value(
                csStat.leaderCount);
            it->second->copysetCount.set_value(
                csStat.copysetCount);
            it->second->readRate.set_value(
                csStat.readRate);
            it->second->writeRate.set_value(
                csStat.writeRate);
            it->second->readIOPS.set_value(
                csStat.readIOPS);
            it->second->writeIOPS.set_value(
                csStat.writeIOPS);
        }
    }

    // 处理逻辑池
    std::vector<PoolIdType> lPools =
        topo_->GetLogicalPoolInCluster([] (const LogicalPool &pool) {
                return pool.GetLogicalPoolAvaliableFlag();
            });
    for (auto pid : lPools) {
        LogicalPool pool;
        if (!topo_->GetLogicalPool(pid, &pool)) {
            continue;
        }
        std::string poolName = pool.GetName();

        std::vector<CopySetInfo> copysets =
            topo_->GetCopySetInfosInLogicalPool(pid);

        std::map<ChunkServerIdType, ChunkServerMetricInfo>
            chunkServerMetricInfo;
        CalcChunkServerMetrics(copysets, &chunkServerMetricInfo);

        auto it = gLogicalPoolMetrics.find(pid);
        if (it == gLogicalPoolMetrics.end()) {
            LogicalPoolMetricPtr lptr(new LogicalPoolMetric(poolName));
            it = gLogicalPoolMetrics.emplace(
                pid, std::move(lptr)).first;
        }

        it->second->chunkServerNum.set_value(
            chunkServerMetricInfo.size());
        it->second->copysetNum.set_value(
            copysets.size());


        LogicalPoolMetricInfo poolMetricInfo;
        CalcLogicalPoolMetrics(chunkServerMetricInfo, &poolMetricInfo);

        it->second->scatterWidthAvg.set_value(
            poolMetricInfo.scatterWidthAvg);
        it->second->scatterWidthVariance.set_value(
            poolMetricInfo.scatterWidthVariance);
        it->second->scatterWidthStandardDeviation.set_value(
            poolMetricInfo.scatterWidthStandardDeviation);
        it->second->scatterWidthRange.set_value(
            poolMetricInfo.scatterWidthRange);
        it->second->scatterWidthMin.set_value(
            poolMetricInfo.scatterWidthMin);
        it->second->scatterWidthMax.set_value(
            poolMetricInfo.scatterWidthMax);
        it->second->copysetNumAvg.set_value(
            poolMetricInfo.copysetNumAvg);
        it->second->copysetNumVariance.set_value(
            poolMetricInfo.copysetNumVariance);
        it->second->copysetNumStandardDeviation.set_value(
            poolMetricInfo.copysetNumStandardDeviation);
        it->second->copysetNumRange.set_value(
            poolMetricInfo.copysetNumRange);
        it->second->copysetNumMin.set_value(
            poolMetricInfo.copysetNumMin);
        it->second->copysetNumMax.set_value(
            poolMetricInfo.copysetNumMax);
        it->second->leaderNumAvg.set_value(
            poolMetricInfo.leaderNumAvg);
        it->second->leaderNumVariance.set_value(
            poolMetricInfo.leaderNumVariance);
        it->second->leaderNumStandardDeviation.set_value(
            poolMetricInfo.leaderNumStandardDeviation);
        it->second->leaderNumRange.set_value(
            poolMetricInfo.leaderNumRange);
        it->second->leaderNumMin.set_value(
            poolMetricInfo.leaderNumMin);
        it->second->leaderNumMax.set_value(
            poolMetricInfo.leaderNumMax);

        uint64_t totalDiskCapacity = 0;
        uint64_t totalDiskUsed = 0;

        // 处理chunkserver的metric,
        // 目前只考虑一个物理池只对应一个逻辑池的情况
        // 因此多个逻辑池内不会有重复的chunkserver
        for (auto cm : chunkServerMetricInfo) {
            auto ix = gChunkServerMetrics.find(cm.first);
            if (ix == gChunkServerMetrics.end()) {
                ChunkServerMetricPtr cptr(
                    new ChunkServerMetric(cm.first));
                ix = gChunkServerMetrics.emplace(
                    cm.first, std::move(cptr)).first;
            }
            ix->second->scatterWidth.set_value(cm.second.scatterWidth);
            ix->second->copysetNum.set_value(cm.second.copysetNum);
            ix->second->leaderNum.set_value(cm.second.leaderNum);

            totalDiskCapacity += ix->second->diskCapacity.get_value();
            totalDiskUsed += ix->second->diskUsed.get_value();
        }

        it->second->diskCapacity.set_value(totalDiskCapacity);
        it->second->diskUsed.set_value(totalDiskUsed);
    }
    // 移除已经不存在的逻辑池metric
    for (auto iy = gLogicalPoolMetrics.begin();
        iy != gLogicalPoolMetrics.end();) {
        if (std::find(lPools.begin(), lPools.end(), iy->first) ==
            lPools.end()) {
            iy = gLogicalPoolMetrics.erase(iy);
        } else {
            iy++;
        }
    }

    // 移除不存在的chunkserver
    for (auto iy = gChunkServerMetrics.begin();
        iy != gChunkServerMetrics.end();) {
        if (std::find(chunkservers.begin(), chunkservers.end(), iy->first) ==
            chunkservers.end()) {
            iy = gChunkServerMetrics.erase(iy);
        } else {
            iy++;
        }
    }

    return;
}

void TopologyMetricService::CalcChunkServerMetrics(
    const std::vector<CopySetInfo> &copysets,
    std::map<ChunkServerIdType, ChunkServerMetricInfo> *csMetricInfoMap) {
    for (auto& cs : copysets) {
        for (auto &csId : cs.GetCopySetMembers()) {
            csMetricInfoMap->emplace(csId, ChunkServerMetricInfo());
        }
    }
    for (auto& pair : *csMetricInfoMap) {
        std::set<ChunkServerIdType> scatterWidthCollector;
        uint32_t leaderCount = 0;
        uint32_t copysetCount = 0;
        for (auto& cs : copysets) {
            std::set<ChunkServerIdType> csMbs =
                cs.GetCopySetMembers();
            if (csMbs.count(pair.first) != 0) {
                scatterWidthCollector.insert(csMbs.begin(),
                    csMbs.end());
                copysetCount++;
            }
            if (cs.GetLeader() == pair.first) {
                leaderCount++;
            }
        }
        // scatterWidth -1 是为了除去自身
        pair.second.scatterWidth = scatterWidthCollector.size() - 1;
        pair.second.copysetNum = copysetCount;
        pair.second.leaderNum = leaderCount;
    }
}

void TopologyMetricService::CalcLogicalPoolMetrics(
    const std::map<ChunkServerIdType, ChunkServerMetricInfo> &csMetricInfoMap,
    LogicalPoolMetricInfo *poolMetricInfo) {
    std::vector<double> scatterWidthVec;
    std::vector<double> copysetNumVec;
    std::vector<double> leaderNumVec;
    for (auto &v : csMetricInfoMap) {
        scatterWidthVec.push_back(v.second.scatterWidth);
        copysetNumVec.push_back(v.second.copysetNum);
        leaderNumVec.push_back(v.second.leaderNum);
    }
    poolMetricInfo->scatterWidthAvg =
        StatisticsTools::CalcAverage(scatterWidthVec);
    poolMetricInfo->scatterWidthVariance =
        StatisticsTools::CalcVariance(scatterWidthVec,
            poolMetricInfo->scatterWidthAvg);
    poolMetricInfo->scatterWidthStandardDeviation =
        StatisticsTools::CalcStandardDevation(
            poolMetricInfo->scatterWidthVariance);
    poolMetricInfo->scatterWidthRange =
        StatisticsTools::CalcRange(scatterWidthVec,
            &poolMetricInfo->scatterWidthMin,
            &poolMetricInfo->scatterWidthMax);

    poolMetricInfo->copysetNumAvg =
        StatisticsTools::CalcAverage(copysetNumVec);
    poolMetricInfo->copysetNumVariance =
        StatisticsTools::CalcVariance(copysetNumVec,
            poolMetricInfo->copysetNumAvg);
    poolMetricInfo->copysetNumStandardDeviation =
        StatisticsTools::CalcStandardDevation(
            poolMetricInfo->copysetNumVariance);
    poolMetricInfo->copysetNumRange =
        StatisticsTools::CalcRange(copysetNumVec,
            &poolMetricInfo->copysetNumMin,
            &poolMetricInfo->copysetNumMax);

    poolMetricInfo->leaderNumAvg =
        StatisticsTools::CalcAverage(leaderNumVec);
    poolMetricInfo->leaderNumVariance =
        StatisticsTools::CalcVariance(leaderNumVec,
            poolMetricInfo->leaderNumAvg);
    poolMetricInfo->leaderNumStandardDeviation =
        StatisticsTools::CalcStandardDevation(
            poolMetricInfo->leaderNumVariance);
    poolMetricInfo->leaderNumRange =
        StatisticsTools::CalcRange(leaderNumVec,
            &poolMetricInfo->leaderNumMin,
            &poolMetricInfo->leaderNumMax);
    return;
}

void TopologyMetricService::BackEndFunc() {
    while (!isStop_.load()) {
        UpdateTopologyMetrics();
        std::this_thread::sleep_for(
            std::chrono::seconds(option_.UpdateMetricIntervalSec));
    }
}


int TopologyMetricService::Init(const TopologyOption &option) {
    option_ = option;
    return 0;
}

int TopologyMetricService::Run() {
    if (isStop_.exchange(false)) {
        backEndThread_ =
            curve::common::Thread(&TopologyMetricService::BackEndFunc, this);
    }
    return 0;
}

int TopologyMetricService::Stop() {
    if (!isStop_.exchange(true)) {
        backEndThread_.join();
    }
    return 0;
}

}  // namespace topology
}  // namespace mds
}  // namespace curve

