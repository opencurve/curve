/*
 * Project: curve
 * Created Date: Fri Dec 21 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <cmath>
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/operatorFactory.h"
#include "src/mds/schedule/scheduler_helper.h"

using ::curve::mds::topology::UNINTIALIZE_ID;

namespace curve {
namespace mds {
namespace schedule {
int CopySetScheduler::Schedule() {
    LOG(INFO) << "copysetScheduler begin";

    int res = 0;
    for (auto lid : topo_->GetLogicalpools()) {
        res = DoCopySetSchedule(lid);
    }
    return res;
}

int CopySetScheduler::DoCopySetSchedule(PoolIdType lid) {
    // 1. 获取集群中copyset 和 chunkserver列表
    //    统计每个online状态chunkserver上的copyset
    auto copysetList = topo_->GetCopySetInfosInLogicalPool(lid);
    auto chunkserverList = topo_->GetChunkServersInLogicalPool(lid);
    std::map<ChunkServerIdType, std::vector<CopySetInfo>> distribute;
    CopySetDistributionInOnlineChunkServer(
        copysetList, chunkserverList, &distribute);
    if (distribute.empty()) {
        LOG(WARNING) << "no not-retired chunkserver in topology";
        return UNINTIALIZE_ID;
    }

    // 2. 获取chunkserver上copyset数量的均值，极差，标准差
    float avg;
    int range;
    float stdvariance;
    StatsCopysetDistribute(distribute, &avg, &range, &stdvariance);

    // 3. 设置迁移条件
    // 条件：极差在均值的百分比范围内，开始迁移
    // 源和目的的选择：
    // - copyset数量多 迁移到 copyset数量少
    // - copyset成员发生变化后，成员上的scatter-with的变化要满足一定的要求
    //   * 成员上的[scatter-with < 最小值]，变动不能使得scatter-with减小
    //   * 成员上的[scatter-with > 最大值]，变动不能使scatter-with更大 //NOLINT
    //   * 成员上的[ 最小值 <= scatter-with <= 最大值]，可以随意变动
    // 最大值和最小值的确定：
    // - 最小值由参数配置
    // - 最大值为最小值上浮一定百分比， 这种确定方式使得极差在一定范围之内
    ChunkServerIdType source = UNINTIALIZE_ID;
    if (range <= avg * copysetNumRangePercent_) {
        return source;
    }

    Operator op;
    ChunkServerIdType target = UNINTIALIZE_ID;
    CopySetInfo choose;
    // 选出copyset、source和target
    if (CopySetMigration(distribute, &op, &source, &target, &choose)) {
        // add operator
        if (!opController_->AddOperator(op)) {
            LOG(INFO) << "copysetSchduler add op " << op.OpToString()
                      << " fail, copyset has already has operator";
        }

        // 创建copyset
        if (!topo_->CreateCopySetAtChunkServer(choose.id, target)) {
            LOG(ERROR) << "copysetScheduler create " << choose.CopySetInfoStr()
                       << " on chunkServer: " << target
                       << " error, delete operator" << op.OpToString();
            opController_->RemoveOperator(choose.id);
        } else {
            LOG(INFO) << "copysetScheduler create " << choose.CopySetInfoStr()
                      << "on chunkserver:" << target
                      << " success. generator op: "
                      << op.OpToString() << "success";
        }
    }

    LOG_EVERY_N(INFO, 20) << "copysetScheduler is continually adjusting";
    LOG(INFO) << "copysetScheduler end.";
    return static_cast<int>(source);
}

void CopySetScheduler::StatsCopysetDistribute(
    const std::map<ChunkServerIdType, std::vector<CopySetInfo>> &distribute,
    float *avg, int *range, float *stdvariance) {
    int num = 0;
    int max = -1;
    int min = -1;
    ChunkServerIdType maxcsId = UNINTIALIZE_ID;
    ChunkServerIdType mincsId = UNINTIALIZE_ID;
    float variance = 0;
    for (auto &item : distribute) {
        num += item.second.size();

        if (max == -1 || item.second.size() > max) {
            max = item.second.size();
            maxcsId = item.first;
        }

        if (min == -1 || item.second.size() < min) {
            min = item.second.size();
            mincsId = item.first;
        }
    }

    // 均值
    *avg = static_cast<float>(num) / distribute.size();

    // 方差
    for (auto &item : distribute) {
        variance += std::pow(static_cast<int>(item.second.size()) - *avg, 2);
    }
    // 极差
    *range = max - min;

    // 均方差
    variance /= distribute.size();
    *stdvariance = std::sqrt(variance);
    LOG(INFO) << "copyset scheduler stats copyset distribute (avg:"
              << *avg << ", {max:" << max << ",maxCsId:" << maxcsId
              << "}, {min:" << min << ",minCsId:" << mincsId
              << "}, range:" << *range << ", stdvariance:" << *stdvariance
              << ", variance:" << variance << ")";
}

void CopySetScheduler::CopySetDistributionInOnlineChunkServer(
    const std::vector<CopySetInfo> &copysetList,
    const std::vector<ChunkServerInfo> &chunkserverList,
    std::map<ChunkServerIdType, std::vector<CopySetInfo>> *out) {
    // 跟据copyset统计每个chunkserver上的copysetList
    for (auto item : copysetList) {
        for (auto peer : item.peers) {
            if (out->find(peer.id) == out->end()) {
                (*out)[peer.id] = std::vector<CopySetInfo>{item};
            } else {
                (*out)[peer.id].emplace_back(item);
            }
        }
    }

    // chunkserver上没有copyset的设置为空, 并且移除offline状态下的copyset
    for (auto item : chunkserverList) {
        if (item.IsOffline()) {
            out->erase(item.info.id);
            continue;
        }
        if (out->find(item.info.id) == out->end()) {
            (*out)[item.info.id] = std::vector<CopySetInfo>{};
        }
    }
}

/**
 * 迁移过程说明:
    chukserverList(chunkserver按照所有copyset的数量从小到大排列):
    {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}[chunkserver-1上copyset数量最少]

 * 目的: 从上述chunkserver中选择chunkserver-n(2<=n<=11),
       从chunkserver-n上选择copyset-m,
       将copyset-m从chunkserver-n迁移到chunkserver-1上,
       即 operator(copyset-m, +chunkserver-1, -chunkserver-n)

 * 问题: 1. 如何确定chunkserver-n   2. 找到chunkserver-n后如何确定copyset-m //NOLINT

 * 步骤:
  for chunkserver-n in {11, 10, 9, 8, 7, 6, 5, 4, 3, 2} (从copyset数量多的开始选) //NOLINT
    for copyset-m in chunkserver-n
        copyset-m的成员(a, b, n)
        1. operator(+1, -n)会对chunkserver{a, b, n 以及 1}上的scatter-width_map造成影响 //NOLINT
           - scatter-width_map的定义是 std::map<chunkserverIdType, int>
           - scatter-width即为len(scatter-width_map)
           - 详细说明:
           chunkserver-a的scatter-width_map:
                key: chunkserver-a上copyset的其他副本所在的chunkserver
                value: key上拥有chunkserver-a上copyset的个数
            例如: chunkserver-a上有copyset-1{a, 1, 2}, copyset-2{a, 2, 3}
            则: scatter-width_map为{{1, 1}, {2, 2}, {3, 1}}, chunkserver-1上
                有copyset-1, 所以value=1; chunkserver-2上有copyset-1和copyset-2, //NOLINT
                所以value=2; chunkserver-3上有copyset-2, 所以value=1.

        2. 对于1(记为target, copyset迁入方)来说: 1的scatter-width_map中key{a,b}对应的value分别+1 //NOLINT
           对于n(记为source, copyset迁出方)来说: n的scatter-width_map中key{a,b}对应的value分别-1 //NOLINT
           对于a和b(记为other)来说: a和b的scatter-width_map中key{1}对应的value+1,{n}对应的value-1 //NOLINT

        3. 判断条件:
           如果迁移之后即copyset-m的成员变为(a, b, 1)后, {a, b, 1, n}的scatter-width //NOLINT
           满足一定的条件, 生成Operator(copyset-m, +chunkserver-1), 等待下发。 //NOLINT
           该operator执行完成之后，replicaScheduler会根据类似的规则选择一个副本将copyset迁出, //NOLINT
           在稳定的情况下，replicaScheduler会选择chunkserver-n迁出.
    done
  done
*/
bool CopySetScheduler::CopySetMigration(
    const std::map<ChunkServerIdType, std::vector<CopySetInfo>> &distribute,
    Operator *op, ChunkServerIdType *source, ChunkServerIdType *target,
    CopySetInfo *choose) {
    if (distribute.size() <= 1) {
        return false;
    }

    // 对distribute进行排序
    std::vector<std::pair<ChunkServerIdType, std::vector<CopySetInfo>>> desc;
    SchedulerHelper::SortDistribute(distribute, &desc);

    // 选择copyset数量最少的作为target, 并获取 info 和 target scatter-with_map
    LOG(INFO) << "copyset schduler after sort (max:" << desc[0].second.size()
        << ",maxCsId:" << desc[0].first
        << "), (min:" << desc[desc.size() - 1].second.size()
        << ",minCsId:" << desc[desc.size() - 1].first << ")";
    *target = desc[desc.size() - 1].first;
    int copysetNumInTarget = desc[desc.size() - 1].second.size();
    if (opController_->ChunkServerExceed(*target)) {
        LOG(INFO) << "copysetScheduler found target:"
                  << *target << " operator exceed";
        return false;
    }

    // 筛选copyset和source
    *source = UNINTIALIZE_ID;
    for (auto it = desc.begin(); it != desc.end()--; it++) {
        // possible souce 和 target上copyset数量相差1, 不应该迁移
        ChunkServerIdType possibleSource = it->first;
        int copysetNumInPossible = it->second.size();
        if (copysetNumInPossible - copysetNumInTarget <= 1) {
            continue;
        }

        for (auto info : it->second) {
            // 不满足基本迁移条件
            if (!CopySetSatisfiyBasicMigrationCond(info)) {
                continue;
            }

            // 该copyset +target,-source之后的各replica的scatter-with是否符合条件 //NOLINT
            if (!SchedulerHelper::SatisfyZoneAndScatterWidthLimit(
                    topo_, *target, possibleSource, info,
                    GetMinScatterWidth(info.id.first),
                    scatterWidthRangePerent_)) {
                continue;
            }

            *source = possibleSource;
            *choose = info;
            break;
        }

        if (*source != UNINTIALIZE_ID) {
            break;
        }
    }

    if (*source != UNINTIALIZE_ID) {
        *op = operatorFactory.CreateChangePeerOperator(
            *choose, *source, *target, OperatorPriority::NormalPriority);
        op->timeLimit = std::chrono::seconds(changeTimeSec_);
        LOG(INFO) << "copyset scheduler gen " << op->OpToString() << " on "
                  << choose->CopySetInfoStr();
        return true;
    }
    return false;
}

bool CopySetScheduler::CopySetSatisfiyBasicMigrationCond(
    const CopySetInfo &info) {
    // copyset上存在operator，不考虑
    Operator exist;
    if (opController_->GetOperatorById(info.id, &exist)) {
        return false;
    }
    if (info.HasCandidate()) {
        LOG(WARNING) << info.CopySetInfoStr()
            << " already has candidate: " << info.candidatePeerInfo.id;
        return false;
    }

    // copyset的replica不是标准数量，不考虑
    if (info.peers.size() !=
        topo_->GetStandardReplicaNumInLogicalPool(info.id.first)) {
        return false;
    }

    // scatter-width在topology中还未设置
    int minScatterWidth = GetMinScatterWidth(info.id.first);
    if (minScatterWidth <= 0) {
        LOG(WARNING) << "minScatterWith in logical pool "
                        << info.id.first << " is not initialized";
        return false;
    }

    // copyset有副本不在线，不考虑
    if (!CopysetAllPeersOnline(info)) {
        return false;
    }

    return true;
}

int64_t CopySetScheduler::GetRunningInterval() {
    return runInterval_;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
