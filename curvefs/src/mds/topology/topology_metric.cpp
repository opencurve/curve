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

#include <algorithm>
#include <list>
#include <memory>
#include <set>
#include <unordered_map>
#include <utility>

namespace curvefs {
namespace mds {
namespace topology {

using FsMetricPtr = std::unique_ptr<FsMetric>;
std::map<PoolIdType, PoolMetricPtr> gPoolMetrics;
std::map<MetaServerIdType, MetaServerMetricPtr> gMetaServerMetrics;
std::map<FsIdType, FsMetricPtr> gFsMetrics;

void TopologyMetricService::UpdateTopologyMetrics() {
    // process metaserver
    std::vector<MetaServerIdType> metaservers =
        topo_->GetMetaServerInCluster([](const MetaServer &ms) {
            (void)ms;
            return true;
        });

    for (auto msId : metaservers) {
        auto it = gMetaServerMetrics.find(msId);
        if (it == gMetaServerMetrics.end()) {
            MetaServerMetricPtr cptr(new MetaServerMetric(msId));
            it = gMetaServerMetrics.emplace(msId, std::move(cptr)).first;
        }
        MetaServer ms;
        if (topo_->GetMetaServer(msId, &ms)) {
            it->second->diskThreshold.set_value(
                ms.GetMetaServerSpace().GetDiskThreshold());
            it->second->diskUsed.set_value(
                ms.GetMetaServerSpace().GetDiskUsed());
            it->second->diskMinRequire.set_value(
                ms.GetMetaServerSpace().GetDiskMinRequire());
            it->second->memoryThreshold.set_value(
                ms.GetMetaServerSpace().GetMemoryThreshold());
            it->second->memoryUsed.set_value(
                ms.GetMetaServerSpace().GetMemoryUsed());
            it->second->memoryMinRequire.set_value(
                ms.GetMetaServerSpace().GetMemoryMinRequire());
        }
    }

    std::unordered_map<FsIdType, uint64_t> fsId2InodeNum;
    std::unordered_map<FsIdType, std::unordered_map<FileType, uint64_t>>
        fsId2FileType2InodeNum;
    // process pool
    std::vector<PoolIdType> pools = topo_->GetPoolInCluster();
    for (auto pid : pools) {
        // prepare pool metrics
        Pool pool;
        if (!topo_->GetPool(pid, &pool)) {
            continue;
        }
        std::string poolName = pool.GetName();
        auto it = gPoolMetrics.find(pid);
        if (it == gPoolMetrics.end()) {
            PoolMetricPtr lptr(new PoolMetric(poolName));
            it = gPoolMetrics.emplace(pid, std::move(lptr)).first;
        }

        // update partitionNum, inodeNum, dentryNum
        uint64_t totalInodeNum = 0;
        uint64_t totalDentryNum = 0;
        std::list<Partition> partitions = topo_->GetPartitionInfosInPool(pid);
        for (auto pit = partitions.begin(); pit != partitions.end(); ++pit) {
            totalInodeNum += pit->GetInodeNum();
            totalDentryNum += pit->GetDentryNum();
            // update fs2inodeNum
            auto fsId = pit->GetFsId();
            auto itFsid2InodeNum = fsId2InodeNum.find(fsId);
            if (itFsid2InodeNum == fsId2InodeNum.end()) {
                fsId2InodeNum.emplace(fsId, pit->GetInodeNum());
            } else {
                itFsid2InodeNum->second += pit->GetInodeNum();
            }
            // update fs2fileType2inodeNum
            auto fileType2InodeNum = pit->GetFileType2InodeNum();
            auto itFsId2FileType2InodeNum = fsId2FileType2InodeNum.find(fsId);
            if (itFsId2FileType2InodeNum == fsId2FileType2InodeNum.end()) {
                fsId2FileType2InodeNum.emplace(fsId,
                                               std::move(fileType2InodeNum));
            } else {
                for (auto const &fileType2Inode : fileType2InodeNum) {
                    auto itFileType2InodeNum =
                        itFsId2FileType2InodeNum->second.find(
                            fileType2Inode.first);
                    if (itFileType2InodeNum ==
                        itFsId2FileType2InodeNum->second.end()) {
                        itFsId2FileType2InodeNum->second.emplace(
                            fileType2Inode);
                    } else {
                        itFileType2InodeNum->second += fileType2Inode.second;
                    }
                }
            }
        }
        it->second->inodeNum.set_value(totalInodeNum);
        it->second->dentryNum.set_value(totalDentryNum);
        it->second->partitionNum.set_value(partitions.size());

        // update copyset
        std::vector<CopySetInfo> copysets = topo_->GetCopySetInfosInPool(pid);
        it->second->copysetNum.set_value(copysets.size());

        // process the metric of metaserver
        std::map<MetaServerIdType, MetaServerMetricInfo> metaServerMetricInfo;
        CalcMetaServerMetrics(copysets, &metaServerMetricInfo);
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
        }

        // update pool resource usage
        uint64_t totalDiskThreshold = 0;
        uint64_t totalDiskUsed = 0;
        uint64_t totalMemoryThreshold = 0;
        uint64_t totalMemoryUsed = 0;
        auto msIdInPool = topo_->GetMetaServerInPool(pid);
        for (auto msId : msIdInPool) {
            auto ix = gMetaServerMetrics.find(msId);
            if (ix == gMetaServerMetrics.end()) {
                MetaServerMetricPtr cptr(new MetaServerMetric(msId));
                ix = gMetaServerMetrics.emplace(msId, std::move(cptr)).first;
            }

            totalDiskThreshold += ix->second->diskThreshold.get_value();
            totalDiskUsed += ix->second->diskUsed.get_value();
            totalMemoryThreshold += ix->second->memoryThreshold.get_value();
            totalMemoryUsed += ix->second->memoryUsed.get_value();

            // process the metric of metaserver which has no copyset
            if (metaServerMetricInfo.find(msId) == metaServerMetricInfo.end()) {
                auto ix = gMetaServerMetrics.find(msId);
                if (ix != gMetaServerMetrics.end()) {
                    ix->second->scatterWidth.set_value(0);
                    ix->second->copysetNum.set_value(0);
                    ix->second->leaderNum.set_value(0);
                    ix->second->partitionNum.set_value(0);
                }
            }
        }

        it->second->metaServerNum.set_value(msIdInPool.size());
        it->second->diskThreshold.set_value(totalDiskThreshold);
        it->second->diskUsed.set_value(totalDiskUsed);
        it->second->memoryThreshold.set_value(totalMemoryThreshold);
        it->second->memoryUsed.set_value(totalMemoryUsed);
    }

    // set fs InodeNum metric
    for (const auto &fsId2InodeNumPair : fsId2InodeNum) {
        auto it = gFsMetrics.find(fsId2InodeNumPair.first);
        if (it == gFsMetrics.end()) {
            FsMetricPtr cptr(new FsMetric(fsId2InodeNumPair.first));
            it = gFsMetrics.emplace(fsId2InodeNumPair.first, std::move(cptr))
                     .first;
        }
        it->second->inodeNum_.set_value(fsId2InodeNumPair.second);
    }

    // remove fs InodeNum metrics that no longer exist
    for (auto it = gFsMetrics.begin(); it != gFsMetrics.end();) {
        if (fsId2InodeNum.find(it->first) == fsId2InodeNum.end()) {
            it = gFsMetrics.erase(it);
        } else {
            ++it;
        }
    }

    // set fsId2FileType2InodeNum metric
    for (auto const &fsId2FileType2InodeNumPair : fsId2FileType2InodeNum) {
        auto it = gFsMetrics.find(fsId2FileType2InodeNumPair.first);
        if (it == gFsMetrics.end()) {
            FsMetricPtr cptr(new FsMetric(fsId2FileType2InodeNumPair.first));
            it = gFsMetrics
                     .emplace(fsId2FileType2InodeNumPair.first, std::move(cptr))
                     .first;
        }
        // set according to fstype
        for (auto const &fileType2InodeNumPair :
             fsId2FileType2InodeNumPair.second) {
            auto it2 = it->second->fileType2InodeNum_.find(
                fileType2InodeNumPair.first);  //  find file type
            if (it2 != it->second->fileType2InodeNum_.end()) {
                // support fs file type
                it2->second->set_value(fileType2InodeNumPair.second);
            }
        }
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
