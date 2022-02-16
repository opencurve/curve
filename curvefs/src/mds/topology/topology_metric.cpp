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
 * Project: curvefs
 * Created Date: 2021/11/1
 * Author: chenwei
 */

#include "curvefs/src/mds/topology/topology_metric.h"

#include <list>
#include <memory>
#include <set>

namespace curvefs {
namespace mds {
namespace topology {

std::map<PoolIdType, PoolMetricPtr> gPoolMetrics;
std::map<MetaServerIdType, MetaServerMetricPtr> gMetaServerMetrics;

void TopologyMetricService::UpdateTopologyMetrics() {
    // process metaserver
    std::vector<MetaServerIdType> metaservers = topo_->GetMetaServerInCluster(
        [](const MetaServer &ms) { return true; });

    for (auto msId : metaservers) {
        auto it = gMetaServerMetrics.find(msId);
        if (it == gMetaServerMetrics.end()) {
            MetaServerMetricPtr cptr(new MetaServerMetric(msId));
            it = gMetaServerMetrics.emplace(msId, std::move(cptr)).first;
        }
        MetaServer ms;
        if (topo_->GetMetaServer(msId, &ms)) {
            it->second->diskCapacity.set_value(
                ms.GetMetaServerSpace().GetDiskThreshold());
            it->second->diskUsed.set_value(
                ms.GetMetaServerSpace().GetDiskUsed());
            it->second->memoryUsed.set_value(
                ms.GetMetaServerSpace().GetMemoryUsed());
        }
    }

    // process pool
    std::vector<PoolIdType> pools = topo_->GetPoolInCluster();
    for (auto pid : pools) {
        Pool pool;
        if (!topo_->GetPool(pid, &pool)) {
            continue;
        }
        std::string poolName = pool.GetName();

        std::vector<CopySetInfo> copysets = topo_->GetCopySetInfosInPool(pid);

        std::map<MetaServerIdType, MetaServerMetricInfo> metaServerMetricInfo;
        CalcMetaServerMetrics(copysets, &metaServerMetricInfo);

        auto it = gPoolMetrics.find(pid);
        if (it == gPoolMetrics.end()) {
            PoolMetricPtr lptr(new PoolMetric(poolName));
            it = gPoolMetrics.emplace(pid, std::move(lptr)).first;
        }

        it->second->metaServerNum.set_value(metaServerMetricInfo.size());
        it->second->copysetNum.set_value(copysets.size());

        uint64_t totalInodeNum = 0;
        uint64_t totalDentryNum = 0;
        std::list<Partition> partitions = topo_->GetPartitionInfosInPool(pid);
        for (auto pit = partitions.begin(); pit != partitions.end(); ++pit) {
            totalInodeNum += pit->GetInodeNum();
            totalDentryNum += pit->GetDentryNum();
        }
        it->second->inodeNum.set_value(totalInodeNum);
        it->second->dentryNum.set_value(totalDentryNum);
        it->second->partitionNum.set_value(partitions.size());

        uint64_t totalDiskCapacity = 0;
        uint64_t totalDiskUsed = 0;

        // process the metric of metaserver.
        for (const auto &cm : metaServerMetricInfo) {
            auto ix = gMetaServerMetrics.find(cm.first);
            if (ix == gMetaServerMetrics.end()) {
                MetaServerMetricPtr cptr(new MetaServerMetric(cm.first));
                ix =
                    gMetaServerMetrics.emplace(cm.first, std::move(cptr)).first;
            }
            ix->second->scatterWidth.set_value(cm.second.scatterWidth);
            ix->second->copysetNum.set_value(cm.second.copysetNum);
            ix->second->leaderNum.set_value(cm.second.leaderNum);
            ix->second->partitionNum.set_value(cm.second.partitionNum);

            totalDiskCapacity += ix->second->diskCapacity.get_value();
            totalDiskUsed += ix->second->diskUsed.get_value();
        }

        it->second->diskCapacity.set_value(totalDiskCapacity);
        it->second->diskUsed.set_value(totalDiskUsed);
    }

    // remove pool metrics that no longer exist
    for (auto it = gPoolMetrics.begin(); it != gPoolMetrics.end();) {
        if (std::find(pools.begin(), pools.end(), it->first) == pools.end()) {
            it = gPoolMetrics.erase(it);
        } else {
            it++;
        }
    }

    // remove metaservers that no longer exist
    for (auto it = gMetaServerMetrics.begin();
         it != gMetaServerMetrics.end();) {
        if (std::find(metaservers.begin(), metaservers.end(), it->first) ==
            metaservers.end()) {
            it = gMetaServerMetrics.erase(it);
        } else {
            it++;
        }
    }

    return;
}

void TopologyMetricService::CalcMetaServerMetrics(
    const std::vector<CopySetInfo> &copysets,
    std::map<MetaServerIdType, MetaServerMetricInfo> *msMetricInfoMap) {
    for (const auto &cs : copysets) {
        for (const auto &msId : cs.GetCopySetMembers()) {
            msMetricInfoMap->emplace(msId, MetaServerMetricInfo());
        }
    }
    for (auto &pair : *msMetricInfoMap) {
        std::set<MetaServerIdType> scatterWidthCollector;
        uint32_t leaderCount = 0;
        uint32_t copysetCount = 0;
        uint32_t partitionNum = 0;
        for (const auto &cs : copysets) {
            std::set<MetaServerIdType> csMbs = cs.GetCopySetMembers();
            if (csMbs.count(pair.first) != 0) {
                scatterWidthCollector.insert(csMbs.begin(), csMbs.end());
                copysetCount++;
                partitionNum += cs.GetPartitionNum();
            }
            if (cs.GetLeader() == pair.first) {
                leaderCount++;
            }
        }
        // scatterWidth - 1 because the metaserver that collect the data of
        // replica number should not be considered according to the definition
        // of scatter width.
        pair.second.scatterWidth = scatterWidthCollector.size() - 1;
        pair.second.copysetNum = copysetCount;
        pair.second.leaderNum = leaderCount;
        pair.second.partitionNum = partitionNum;
    }
}

void TopologyMetricService::BackEndFunc() {
    while (sleeper_.wait_for(
        std::chrono::seconds(option_.UpdateMetricIntervalSec))) {
        UpdateTopologyMetrics();
    }
}

void TopologyMetricService::Init(const TopologyOption &option) {
    option_ = option;
}

void TopologyMetricService::Run() {
    if (isStop_.exchange(false)) {
        backEndThread_ =
            curve::common::Thread(&TopologyMetricService::BackEndFunc, this);
    }
}

void TopologyMetricService::Stop() {
    if (!isStop_.exchange(true)) {
        LOG(INFO) << "stop TopologyMetricService...";
        sleeper_.interrupt();
        backEndThread_.join();
        LOG(INFO) << "stop TopologyMetricService ok.";
    }
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
