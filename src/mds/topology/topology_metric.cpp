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
 * Created Date: Thu Jun 27 2019
 * Author: xuchaojie
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
ClusterMetricPtr gClusterMetrics =
  std::unique_ptr<ClusterMetric>(new ClusterMetric);

void TopologyMetricService::UpdateTopologyMetrics() {
    // process chunkserver
    std::vector<ChunkServerIdType> chunkservers =
        topo_->GetChunkServerInCluster(
                [](const ChunkServer &cs) {
                    return cs.GetStatus() != ChunkServerStatus::RETIRED;
                });

    std::vector<CopysetStat> copysetStatsCs, copysetStatsCluster;
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
            it->second->chunkSizeUsedBytes.set_value(
                csStat.chunkSizeUsedBytes);
            it->second->chunkSizeLeftBytes.set_value(
                csStat.chunkSizeLeftBytes);
            it->second->chunkSizeTrashedBytes.set_value(
                csStat.chunkSizeTrashedBytes);
            it->second->chunkSizeTotalBytes.set_value(
                csStat.chunkSizeUsedBytes +
                csStat.chunkSizeLeftBytes +
                csStat.chunkSizeTrashedBytes);
        }

        copysetStatsCs = csStat.copysetStats;
        copysetStatsCluster.insert(copysetStatsCluster.end(),
          copysetStatsCs.begin(), copysetStatsCs.end());
        copysetStatsCs.clear();
    }

    // process logical pool
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

        auto servers = topo_->GetServerInLogicalPool(pid);
        it->second->serverNum.set_value(servers.size());
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
        uint64_t totalChunkSizeUsedBytes = 0;
        uint64_t totalChunkSizeLeftBytes = 0;
        uint64_t totalChunkSizeTrashedBytes = 0;
        uint64_t totalChunkSizeBytes = 0;

        // process the metric of chunkserver.
        // by now we only consider the case that a physical pool only
        // corresponds to one logical pool, thus there won't be any duplicate
        // chunkservers between logical pools
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
            totalChunkSizeUsedBytes +=
                ix->second->chunkSizeUsedBytes.get_value();
            totalChunkSizeLeftBytes +=
                ix->second->chunkSizeLeftBytes.get_value();
            totalChunkSizeTrashedBytes +=
                ix->second->chunkSizeTrashedBytes.get_value();
            totalChunkSizeBytes +=
                ix->second->chunkSizeTotalBytes.get_value();
        }

        it->second->diskCapacity.set_value(totalDiskCapacity);
        it->second->diskUsed.set_value(totalDiskUsed);
        int64_t diskAlloc = 0;
        allocStatistic_->GetAllocByLogicalPool(pid, &diskAlloc);
        it->second->logicalAlloc.set_value(diskAlloc);
        // replica number should be considered
        it->second->diskAlloc.set_value(diskAlloc * pool.GetReplicaNum());

        it->second->chunkSizeUsedBytes.set_value(totalChunkSizeUsedBytes);
        it->second->chunkSizeLeftBytes.set_value(totalChunkSizeLeftBytes);
        it->second->chunkSizeTrashedBytes.set_value(totalChunkSizeTrashedBytes);
        it->second->chunkSizeTotalBytes.set_value(totalChunkSizeBytes);
        if (pool.GetReplicaNum() != 0) {
            it->second->logicalCapacity.set_value(
                totalChunkSizeBytes / pool.GetReplicaNum());
        }

        uint64_t readRate = 0, writeRate = 0,
          readIOPS = 0, writeIOPS = 0;
        for (auto iterCsStat : copysetStatsCluster) {
            if (iterCsStat.logicalPoolId == pid) {
                readRate += iterCsStat.readRate;
                writeRate += iterCsStat.writeRate;
                readIOPS += iterCsStat.readIOPS;
                writeIOPS += iterCsStat.writeIOPS;
            }
            DVLOG(6) << "copyset Metrics, csid is: "
                  << iterCsStat.copysetId << ", write iops: "
                  << iterCsStat.writeIOPS << ", write bps: "
                  << iterCsStat.writeRate << ", read bps: "
                  << iterCsStat.readRate  << ", read iops: "
                  << iterCsStat.readIOPS;
        }
        it->second->writeIOPS.set_value(writeIOPS);
        it->second->writeRate.set_value(writeRate);
        it->second->readRate.set_value(readRate);
        it->second->readIOPS.set_value(readIOPS);

        DVLOG(6) << "pool metrics, pid is: "
            << pid << ", write iops: "
            << writeIOPS << ", write bps: "
            << writeRate << ", read bps: "
            << readRate  << ", read iops: "
            << readIOPS;
    }

    // remove logical pool metrics that no longer exist
    for (auto iy = gLogicalPoolMetrics.begin();
        iy != gLogicalPoolMetrics.end();) {
        if (std::find(lPools.begin(), lPools.end(), iy->first) ==
            lPools.end()) {
            iy = gLogicalPoolMetrics.erase(iy);
        } else {
            iy++;
        }
    }

    // remove chunkservers that no longer exist
    for (auto iy = gChunkServerMetrics.begin();
        iy != gChunkServerMetrics.end();) {
        if (std::find(chunkservers.begin(), chunkservers.end(), iy->first) ==
            chunkservers.end()) {
            iy = gChunkServerMetrics.erase(iy);
        } else {
            iy++;
        }
    }

    // cluster metric
    uint64_t clusterReadIOPS = 0, clusterWriteIOPS = 0,
        clusterReadRate = 0, clusterWriteRate = 0, copysetNum = 0;
    uint32_t serverNum = 0;
    uint64_t logicalCapacity = 0, logicalAlloc = 0;
    for (auto metricIter = gLogicalPoolMetrics.begin();
        metricIter != gLogicalPoolMetrics.end(); metricIter++) {
        clusterReadIOPS += metricIter->second->readIOPS.get_value();
        clusterWriteIOPS +=  metricIter->second->writeIOPS.get_value();
        clusterReadRate +=  metricIter->second->readRate.get_value();
        clusterWriteRate +=  metricIter->second->writeRate.get_value();
        copysetNum += metricIter->second->copysetNum.get_value();
        serverNum += metricIter->second->serverNum.get_value();
        logicalCapacity += metricIter->second->logicalCapacity.get_value();
        logicalAlloc += metricIter->second->logicalAlloc.get_value();
    }

    gClusterMetrics->serverNum.set_value(serverNum);
    gClusterMetrics->chunkServerNum.set_value(chunkservers.size());
    gClusterMetrics->copysetNum.set_value(copysetNum);
    gClusterMetrics->logicalPoolNum.set_value(lPools.size());
    gClusterMetrics->readIOPS.set_value(clusterReadIOPS);
    gClusterMetrics->writeIOPS.set_value(clusterWriteIOPS);
    gClusterMetrics->readRate.set_value(clusterReadRate);
    gClusterMetrics->writeRate.set_value(clusterWriteRate);
    gClusterMetrics->logicalCapacity.set_value(logicalCapacity);
    gClusterMetrics->logicalAlloc.set_value(logicalAlloc);



    DVLOG(6) << "cluster metrics, logical pool num: "
            << gClusterMetrics->logicalPoolNum.get_value()
            << ", cluster chunkserver num: "
            << gClusterMetrics->chunkServerNum.get_value()
            << ", cluster copyset num: "
            << gClusterMetrics->copysetNum.get_value()
            << ", cluster readiops: "
            << gClusterMetrics->readIOPS.get_value()
            << ", cluster writeiops: "
            << gClusterMetrics->writeIOPS.get_value()
            << ", cluster readbps: "
            << gClusterMetrics->readRate.get_value()
            << ", cluster writebps: "
            << gClusterMetrics->writeRate.get_value();
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
        // scatterWidth - 1 because the chunkserver that collect the data of
        // replica number should not be considered according to the definition
        // of scatter width.
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
    while (sleeper_.wait_for(
        std::chrono::seconds(option_.UpdateMetricIntervalSec))) {
        UpdateTopologyMetrics();
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
        LOG(INFO) << "stop TopologyMetricService...";
        sleeper_.interrupt();
        backEndThread_.join();
        LOG(INFO) << "stop TopologyMetricService ok.";
    }
    return 0;
}

}  // namespace topology
}  // namespace mds
}  // namespace curve

