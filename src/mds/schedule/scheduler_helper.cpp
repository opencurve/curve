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
 * Created Date: Thur July 06th 2019
 * Author: lixiaocui
 */

#include <glog/logging.h>
#include <algorithm>
#include <random>
#include <memory>
#include <utility>
#include "src/mds/schedule/scheduler_helper.h"

namespace curve {
namespace mds {
namespace schedule {

/**
 * for operation copyset(ABC),-C,+D, C is the source and D is the target.
 * this operation will change the scatter-width of {ABCD}, and for old
 * scatter-width oldValue and new scatterwidth newValue, the change should
 * satisfy one of the conditions below:
 * 1. for A,B,C
 *   ① minScatterWidth<=newValue<=maxScatterWidth
 *   ② if newValue<minScatterWidth, the scatter-width should not decrease
 *     (newValue-oldValue>=0)
 *   ③ if newValue>maxScatterWidth, the scatter-width should not increase
 *     (newValue-oldValue<=0)
 * 2. for D
 *   ① minScatterWidth<=newValue<=maxScatterWidth
 *   ② if newValue<minScatterWidth, the scatter-width should increase at least 1
 *     (newValue-oldValue>=1)
 *   ③ if newValue>maxScatterWidth, the scatter-width should decrease at least 1
 *     (newValue-oldValue<=-1)
 */
bool SchedulerHelper::SatisfyScatterWidth(
    bool target, int oldValue, int newValue,
    int minScatterWidth, float scatterWidthRangePerent) {
    int maxValue = minScatterWidth * (1 + scatterWidthRangePerent);
    // newValue is smaller than the minimum value
    if (newValue < minScatterWidth) {
        // not the target
        if (!target) {
            // newValue decrease, invalid
            if (newValue - oldValue < 0) {
                return false;
            }
        // target
        } else {
            // the increase of scatter-width is less than 1, invalid
            if (newValue - oldValue < 1) {
                return false;
            }
        }
    // newValue is larger than the maximum value
    } else if (newValue > maxValue) {
        // not the target
        if (!target) {
            // newValue increase increase, invalid
            if (newValue - oldValue > 0) {
                return false;
            }
        // target
        } else {
            // the scatter-width decrease is less than 1, invalid
            if (newValue - oldValue >= 0) {
                return false;
            }
        }
    }
    return true;
}


bool SchedulerHelper::SatisfyZoneAndScatterWidthLimit(
    const std::shared_ptr<TopoAdapter> &topo, ChunkServerIdType target,
    ChunkServerIdType source, const CopySetInfo &candidate,
    int minScatterWidth, float scatterWidthRangePerent) {
    ChunkServerInfo targetInfo;
    if (!topo->GetChunkServerInfo(target, &targetInfo)) {
        LOG(ERROR) << "copyset scheduler can not get chunkserver " << target;
        return false;
    }
    ZoneIdType targetZone = targetInfo.info.zoneId;
    ZoneIdType sourceZone;
    int minZone = topo->GetStandardZoneNumInLogicalPool(candidate.id.first);
    if (minZone <=0) {
        LOG(ERROR) << "standard zone num should > 0";
        return false;
    }

    // the zone list stores the number chunkserver of the copyset given that a
    // zone has
    std::map<ZoneIdType, int> zoneList;

    for (auto info : candidate.peers) {
        // update zoneList
        if (zoneList.count(info.zoneId) > 0) {
            zoneList[info.zoneId] += 1;
        } else {
            zoneList[info.zoneId] = 1;
        }

        // record the source zone
        if (source == info.id) {
            sourceZone = info.zoneId;
        }
    }

    // calculate the zoneList after the migration and determine whether it
    // satisfy the condition
    if (zoneList.count(sourceZone) >= 1) {
        if (zoneList[sourceZone] == 1) {
            zoneList.erase(sourceZone);
        } else {
            zoneList[sourceZone] -= 1;
        }
    }

    if (zoneList.count(targetZone) == 0) {
        zoneList[targetZone] = 1;
    } else {
        zoneList[targetZone] += 1;
    }

    if (static_cast<int>(zoneList.size()) < minZone) {
        return false;
    }

    // determine whether the scatter-width of source(-other)/target(+other)/other(-source, +target) satisfy the requirement //NOLINT
    int affected = 0;
    if (SchedulerHelper::InvovledReplicasSatisfyScatterWidthAfterMigration(
                candidate, source, target, UNINTIALIZE_ID, topo,
                minScatterWidth, scatterWidthRangePerent, &affected)) {
        return true;
    }
    return false;
}

void SchedulerHelper::SortDistribute(
    const std::map<ChunkServerIdType, std::vector<CopySetInfo>> &distribute,
    std::vector<std::pair<ChunkServerIdType, std::vector<CopySetInfo>>> *desc) {
    static std::random_device rd;
    static std::mt19937 g(rd());

    for (auto item : distribute) {
        std::shuffle(item.second.begin(), item.second.end(), g);
        desc->emplace_back(
            std::pair<ChunkServerIdType, std::vector<CopySetInfo>>(
                item.first, item.second));
    }
    std::shuffle(desc->begin(), desc->end(), g);

    std::sort(desc->begin(), desc->end(),
        [](const std::pair<ChunkServerIdType, std::vector<CopySetInfo>> &c1,
           const std::pair<ChunkServerIdType, std::vector<CopySetInfo>> &c2) {
            return c1.second.size() > c2.second.size();
    });
}

void SchedulerHelper::SortChunkServerByCopySetNumAsc(
    std::vector<ChunkServerInfo> *chunkserverList,
    const std::shared_ptr<TopoAdapter> &topo) {
    std::vector<CopySetInfo> copysetList = topo->GetCopySetInfos();
    // calculate copyset number of chunkservers
    std::map<ChunkServerIdType, int> copysetNumInCs;
    for (auto copyset : copysetList) {
        for (auto peer : copyset.peers) {
            if (copysetNumInCs.find(peer.id) == copysetNumInCs.end()) {
                copysetNumInCs[peer.id] = 1;
            } else {
                copysetNumInCs[peer.id] += 1;
            }
        }
    }

    // transfer map to vector
    std::vector<std::pair<ChunkServerInfo, int>> transfer;
    for (auto &csInfo : *chunkserverList) {
        int num = 0;
        if (copysetNumInCs.find(csInfo.info.id) != copysetNumInCs.end()) {
            num = copysetNumInCs[csInfo.info.id];
        }
        std::pair<ChunkServerInfo, int> item{csInfo, num};
        transfer.emplace_back(item);
    }

    // randomize chunkserverlist
    static std::random_device rd;
    static std::mt19937 g(rd());
    std::shuffle(transfer.begin(), transfer.end(), g);

    // sort
    std::sort(transfer.begin(), transfer.end(),
        [](const std::pair<ChunkServerInfo, int> &c1,
           const std::pair<ChunkServerInfo, int> &c2){
            return c1.second < c2.second;});

    // place back to chunkserverList
    chunkserverList->clear();
    for (auto item : transfer) {
        chunkserverList->emplace_back(item.first);
    }
}

void SchedulerHelper::SortScatterWitAffected(
    std::vector<std::pair<ChunkServerIdType, int>> *candidates) {
    static std::random_device rd;
    static std::mt19937 g(rd());
    std::shuffle(candidates->begin(), candidates->end(), g);

    std::sort(candidates->begin(), candidates->end(),
        [](const std::pair<ChunkServerIdType, int> &c1,
           const std::pair<ChunkServerIdType, int> &c2) {
            return c1.second < c2.second;
    });
}

/**
 * for copyset(ABC), calculate the influence of operation
 * {-C, +D} to scatter-width
 * for A: -C,+D
 * for B: -C,+D
 * for C: -A,-B
 * for D: +A,+B
 */
void SchedulerHelper::CalculateAffectOfMigration(
    const CopySetInfo &copySetInfo, ChunkServerIdType source,
    ChunkServerIdType target, const std::shared_ptr<TopoAdapter> &topo,
    std::map<ChunkServerIdType, std::pair<int, int>> *scatterWidth) {
    // get scatter-width map and scatter-width of target
    std::map<ChunkServerIdType, int> targetMap;
    if (target != UNINTIALIZE_ID) {
        topo->GetChunkServerScatterMap(target, &targetMap);
        (*scatterWidth)[target].first = targetMap.size();
    }

    // get scatter-width map and scatter-width of the source
    std::map<ChunkServerIdType, int> sourceMap;
    if (source != UNINTIALIZE_ID) {
        topo->GetChunkServerScatterMap(source, &sourceMap);
        (*scatterWidth)[source].first = sourceMap.size();
    }

    // calculate the influence between otherList {A,B} and {C,D}
    for (PeerInfo peer : copySetInfo.peers) {
        if (peer.id == source) {
            continue;
        }

        std::map<ChunkServerIdType, int> tmpMap;
        topo->GetChunkServerScatterMap(peer.id, &tmpMap);
        (*scatterWidth)[peer.id].first = tmpMap.size();
        // if target was initialized
        if (target != UNINTIALIZE_ID) {
            // influence on target
            if (targetMap.count(peer.id) == 0) {
                targetMap[peer.id] = 1;
            } else {
                targetMap[peer.id]++;
            }

            // target's influence on other chunkservers
            if (tmpMap.count(target) == 0) {
                tmpMap[target] = 1;
            } else {
                tmpMap[target]++;
            }
        }

        // if source was initialized
        if (source != UNINTIALIZE_ID) {
            // influence on source
            if (tmpMap[source] <= 1) {
                tmpMap.erase(source);
            } else {
                tmpMap[source]--;
            }

            // influence of source on other chunkservers
            if (sourceMap[peer.id] <= 1) {
                sourceMap.erase(peer.id);
            } else {
                sourceMap[peer.id]--;
            }
        }
        (*scatterWidth)[peer.id].second = tmpMap.size();
    }

    if (target != UNINTIALIZE_ID) {
        (*scatterWidth)[target].second = targetMap.size();
    }

    if (source != UNINTIALIZE_ID) {
        (*scatterWidth)[source].second = sourceMap.size();
    }
}

bool SchedulerHelper::InvovledReplicasSatisfyScatterWidthAfterMigration(
    const CopySetInfo &copySetInfo, ChunkServerIdType source,
    ChunkServerIdType target, ChunkServerIdType ignore,
    const std::shared_ptr<TopoAdapter> &topo,
    int minScatterWidth, float scatterWidthRangePerent, int *affected) {
    // calculate the influence of copyset(A,B,source),+target,-source
    // on the scatter-width of {A,B,source,target}
    std::map<ChunkServerIdType, std::pair<int, int>> out;
    SchedulerHelper::CalculateAffectOfMigration(
            copySetInfo, source, target, topo, &out);

    // judge the validity of the scatter-width change
    bool allSatisfy = true;
    *affected = 0;
    for (auto iter = out.begin(); iter != out.end(); iter++) {
        // if the source chunkserver is offline, we don't have to care about
        // whether the change of moving out from source is valid
        if (iter->first == ignore) {
            continue;
        }

        int beforeMigrationScatterWidth = iter->second.first;
        int afterMigrationScatterWidth = iter->second.second;
        if (!SchedulerHelper::SatisfyScatterWidth(
            iter->first == target,
            beforeMigrationScatterWidth,
            afterMigrationScatterWidth,
            minScatterWidth, scatterWidthRangePerent)) {
            allSatisfy = false;
        }

        // calculate the sum of the change of scatter-width
        *affected +=
            afterMigrationScatterWidth - beforeMigrationScatterWidth;
    }

    return allSatisfy;
}

void SchedulerHelper::GetCopySetDistributionInOnlineChunkServer(
        const std::vector<CopySetInfo> &copysetList,
        const std::vector<ChunkServerInfo> &chunkserverList,
        std::map<ChunkServerIdType, std::vector<CopySetInfo>> *out) {
    // calculate the copysetlist by traversing the copyset list
    for (auto item : copysetList) {
        for (auto peer : item.peers) {
            if (out->find(peer.id) == out->end()) {
                (*out)[peer.id] = std::vector<CopySetInfo>{item};
            } else {
                (*out)[peer.id].emplace_back(item);
            }
        }
    }

    // remove offline chunkserver, and report empty list for empty chunkserver
    for (auto item : chunkserverList) {
        // remove offline chunkserver
        if (item.IsOffline()) {
            out->erase(item.info.id);
            continue;
        }

        // report empty list for chunkserver with no copyset
        if (out->find(item.info.id) == out->end()) {
            (*out)[item.info.id] = std::vector<CopySetInfo>{};
        }
    }
}

void SchedulerHelper::FilterCopySetDistributions(const ChunkServerStatus status,
        const std::vector<ChunkServerInfo> &chunkserverList,
        std::map<ChunkServerIdType, std::vector<CopySetInfo>> *distributions) {
    for (auto item : chunkserverList) {
        if (item.status != status) {
            distributions->erase(item.info.id);
        }
    }
    return;
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve


