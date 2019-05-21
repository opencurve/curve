/*
 * Project: curve
 * Created Date: Thur July 06th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <algorithm>
#include <memory>
#include <utility>
#include "src/mds/schedule/scheduler_helper.h"

namespace curve {
namespace mds {
namespace schedule {
/**
 copyset(ABC),-C,+D, C称为source, D称为target.
 该操作对chunkserver{ABCD}的scatter-width有影响.
 变更之前的scatter-width记为oldValue, 变更后记为newValue.
 该变更需要符合以下条件：
 1. 对于A,B,C
    ① 变更之后的值在标准范围之内 minScatterWidth<=newValue<=maxScatterWidth
    ② 变更之后值小于最小值 newValue<minScatterWidth, 但变更没有使得scatter-width减小
       newValue-oldValue>=0
    ③ 变更之后值大于最大值 newValue>maxScatterWidth, 但变更没有使得scatter-width增大
       newValue-oldValue<=0
 2. 对于D
    ① 变更之后值在标准范围之内 minScatterWidth<=newValue<=maxScatterWidth
    ② 变更之后值小于最小值 newValue<minScatterWidth, 但变更要使得scatter-with的值
       至少增加1 newValue-oldValue>=1
    ③ 变更之后值大于最大值 newValue>maxScatterWidth, 但变更要使得scatter-width的值
       至少减小1 newValue-oldValue<=-1
 */
bool SchedulerHelper::SatisfyScatterWidth(
    bool target, int oldValue, int newValue,
    int minScatterWidth, float scatterWidthRangePerent) {
    int maxValue = minScatterWidth * (1 + scatterWidthRangePerent);
    // 变更之后的scatter-with小于最小值
    if (newValue < minScatterWidth) {
        // 非target
        if (!target) {
            // 非target,变更之后值变小，不符合条件
            if (newValue - oldValue < 0) {
                return false;
            }
        // target
        } else {
            // target, 变更之后没有使得scatter-with至少增加1，不符合条件
            if (newValue - oldValue < 1) {
                return false;
            }
        }
    // 变更之后的scatter-with大于最大值
    } else if (newValue > maxValue) {
        // 非target
        if (!target) {
            // 非target，变更之后值变大，不符合条件
            if (newValue - oldValue > 0) {
                return false;
            }
        // target
        } else {
            // target, 变更之后没有使得scatter-with至少减少1，不符合条件
            if (newValue - oldValue >= 0) {
                return false;
            }
        }
    }
    // 变更之后的scatter-with在[min, max]之间
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
    ZoneIDType targetZone = targetInfo.info.zoneId;
    ZoneIDType sourceZone;
    int minZone = topo->GetStandardZoneNumInLogicalPool(candidate.id.first);
    if (minZone <=0) {
        LOG(ERROR) << "standard zone num should >0";
        return false;
    }

    // 非source和非target
    std::map<ZoneIdType, int> zoneList;

    for (auto info : candidate.peers) {
        // 更新zoneList
        if (zoneList.count(info.zoneId) > 0) {
            zoneList[info.zoneId] += 1;
        } else {
            zoneList[info.zoneId] = 1;
        }

        // 记录source zone
        if (source == info.id) {
            sourceZone = info.zoneId;
        }
    }

    // 迁移过后是否符合zone条件, -sourceZone, +targetZone
    if (zoneList.count(sourceZone) >= 1) {
        if (zoneList[sourceZone] == 1) {
            zoneList.erase(sourceZone);
        } else {
            zoneList[sourceZone] -= 1;
        }
    }

    if (zoneList.count(targetZone) <= 0) {
        zoneList[targetZone] = 1;
    } else {
        zoneList[targetZone] += 1;
    }

    if (zoneList.size() < minZone) {
        return false;
    }

    // 迁移过后source(-other)/target(+other)上的scatter-with是否符合要求
    // 迁移过后的otherReplica是否符合要求，-source, +target
    int affected = 0;
    if (SchedulerHelper::InvovledReplicasSatisfyScatterWidthAfterMigration(
                candidate, source, target, UNINTIALIZE_ID, topo,
                minScatterWidth, scatterWidthRangePerent, &affected)) {
        return true;
    }
    return false;
}

void SchedulerHelper::SortDistribute(
    const std::map<ChunkServerIDType, std::vector<CopySetInfo>> &distribute,
    std::vector<std::pair<ChunkServerIDType, std::vector<CopySetInfo>>> *desc) {
    for (auto item : distribute) {
        desc->emplace_back(
            std::pair<ChunkServerIDType, std::vector<CopySetInfo>>(
                item.first, item.second));
    }

    std::sort(desc->begin(), desc->end(),
        [](const std::pair<ChunkServerIDType, std::vector<CopySetInfo>> &c1,
           const std::pair<ChunkServerIDType, std::vector<CopySetInfo>> &c2) {
            return c1.second.size() > c2.second.size();
    });
}

void SchedulerHelper::SortScatterWitAffected(
    std::vector<std::pair<ChunkServerIdType, int>> *candidates) {
    std::sort(candidates->begin(), candidates->end(),
        [](const std::pair<ChunkServerIdType, int> &c1,
           const std::pair<ChunkServerIdType, int> &c2) {
            return c1.second < c2.second;
    });
}

/**
 copyset(ABC),-C,+D操作对于{ABCD}的影响:
 对于A: -C,+D
 对于B: -C,+D
 对于C: -A,-B
 对于D: +A,+B
 */
void SchedulerHelper::CalculateAffectOfMigration(
    const CopySetInfo &copySetInfo, ChunkServerIdType source,
    ChunkServerIdType target, const std::shared_ptr<TopoAdapter> &topo,
    std::map<ChunkServerIdType, std::pair<int, int>> *scatterWidth) {
    // 获取target的scatter-width map
    std::map<ChunkServerIdType, int> targetMap;
    std::pair<int, int> targetScatterWidth;
    if (target != UNINTIALIZE_ID) {
        topo->GetChunkServerScatterMap(target, &targetMap);
        (*scatterWidth)[target].first = targetMap.size();
    }

    // 获取source的scatter-width map
    std::map<ChunkServerIdType, int> sourceMap;
    std::pair<int, int> sourceScatterWidth;
    if (source != UNINTIALIZE_ID) {
        topo->GetChunkServerScatterMap(source, &sourceMap);
        (*scatterWidth)[source].first = sourceMap.size();
    }

    // 计算otherList{A,B}对C,D产生的影响 以及 受到C,D的影响
    for (PeerInfo peer : copySetInfo.peers) {
        if (peer.id == source) {
            continue;
        }

        std::map<ChunkServerIdType, int> tmpMap;
        topo->GetChunkServerScatterMap(peer.id, &tmpMap);
        (*scatterWidth)[peer.id].first = tmpMap.size();
        // 如果target被初始化
        if (target != UNINTIALIZE_ID) {
            // 对于target的影响， +replica
            if (targetMap.count(peer.id) <= 0) {
                targetMap[peer.id] = 1;
            } else {
                targetMap[peer.id]++;
            }

            // target对于其他副本的影响，+target
            if (tmpMap.count(target) <= 0) {
                tmpMap[target] = 1;
            } else {
                tmpMap[target]++;
            }
        }

        // 如果source被初始化
        if (source != UNINTIALIZE_ID) {
            // source对于其他副本的影响，-source
            if (tmpMap[source] <= 1) {
                tmpMap.erase(source);
            } else {
                tmpMap[source]--;
            }

            // 对于source的影响，-replica
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
    // 计算copyset(A,B,source),+target,-source对{A,B,source,target}的
    // scatter-width产生的影响
    std::map<ChunkServerIdType, std::pair<int, int>> out;
    SchedulerHelper::CalculateAffectOfMigration(
            copySetInfo, source, target, topo, &out);

    // 判断{A,B,source,target}上scatter-width的变动是否符合条件
    bool allSatisfy = true;
    *affected = 0;
    for (auto iter = out.begin(); iter != out.end(); iter++) {
        // 如果source是offline状态, 不需要关心copyset从source上迁移之后是否满足条件 //NOLINT
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

        // 计算该变更对所有副本scatter-width的影响之和
        *affected +=
            afterMigrationScatterWidth - beforeMigrationScatterWidth;
    }

    return allSatisfy;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve


