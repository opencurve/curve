/*
 * Project: curve
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/mds/heartbeat/topo_updater.h"

namespace curve {
namespace mds {
namespace heartbeat {
void TopoUpdater::UpdateTopo(const CopySetInfo &reportCopySetInfo) {
    CopySetInfo recordCopySetInfo;
    if (!topo_->GetCopySet(
        reportCopySetInfo.GetCopySetKey(), &recordCopySetInfo)) {
        LOG(ERROR) << "topoUpdater receive copyset("
                   << reportCopySetInfo.GetLogicalPoolId()
                   << "," << reportCopySetInfo.GetId()
                   << ") information, but can not get info from topology";
        return;
    }

    // 比较report epoch 和 mds record epoch的大小，有如下三种情况:
    // 1. report epoch > mds record epoch
    //    每完成一次配置变更，raft会增加copyset的epoch, 此时mds的epoch将会落后
    //    需要将新的epoch和copyset副本关系更新到mds
    // 2. report epoch == mds record epoch
    //    没有配置变更或配置变更正在进行. 可能需要将candidate记录到mds中
    // 3. report epoch < mds record epoch
    //    这种情况不应该发生，raft中的epoch永远是最新的. 不排除代码bug导致，需要报警 //NOLINT

    // mds epoch落后，需要更新.
    bool needUpdate = false;
    if (recordCopySetInfo.GetEpoch() < reportCopySetInfo.GetEpoch()) {
        LOG(INFO) << "topoUpdater find report copyset("
                  << reportCopySetInfo.GetLogicalPoolId()
                  << "," << reportCopySetInfo.GetId()
                  << ") epoch:" << reportCopySetInfo.GetEpoch()
                  << " > recordEpoch:"
                  << recordCopySetInfo.GetEpoch() << " need to update";
        needUpdate = true;
    } else if (recordCopySetInfo.GetEpoch() == reportCopySetInfo.GetEpoch()) {
        // 上报的epoch和记录的epoch相等.

        // report中leader和topology记录的leader不一致
        if (reportCopySetInfo.GetLeader() != recordCopySetInfo.GetLeader()) {
            needUpdate = true;
        }

        // report中memberlist 和 record的不一致, 这种情况应该报警
        // 这种情况不应该存在，chunkserver端总是返回已经apply的配置
        // 因此，member list不一样的情况epoch一定不同
        if (reportCopySetInfo.GetCopySetMembers() !=
            recordCopySetInfo.GetCopySetMembers()) {
            LOG(ERROR) << "topoUpdater find report copyset("
                       << reportCopySetInfo.GetLogicalPoolId()
                       << "," << reportCopySetInfo.GetId()
                       << ") member list: "
                       << reportCopySetInfo.GetCopySetMembersStr()
                       << " is not same as record one: "
                       << recordCopySetInfo.GetCopySetMembersStr()
                       << ", but epoch is same: "
                       << recordCopySetInfo.GetEpoch();
            return;
        }

        // report的信息中不含有变更项
        if (!reportCopySetInfo.HasCandidate()) {
            // report信息中不含变更项，但mds上有
            if (recordCopySetInfo.HasCandidate()) {
                LOG(WARNING) << "topoUpdater find report"
                             " copyset("
                             << reportCopySetInfo.GetLogicalPoolId()
                             << "," << reportCopySetInfo.GetId()
                             << ") no candidate but record has candidate: "
                             << recordCopySetInfo.GetCandidate();
                needUpdate = true;
            }
        } else if (!recordCopySetInfo.HasCandidate()) {
            // report有变更项，但是mds上没有
            needUpdate = true;
        } else if (reportCopySetInfo.GetCandidate() !=
                   recordCopySetInfo.GetCandidate()) {
            // report的变更项和mds记录的不同
            LOG(WARNING) << "topoUpdater find report candidate "
                         << reportCopySetInfo.GetCandidate()
                         << ", record candidate: "
                         << recordCopySetInfo.GetCandidate()
                         << " on copyset("
                         << reportCopySetInfo.GetLogicalPoolId()
                         << "," << reportCopySetInfo.GetId()
                         << ") not same";
            needUpdate = true;
        }
    } else if (recordCopySetInfo.GetEpoch() > reportCopySetInfo.GetEpoch()) {
        // report epoch小于 mds记录的epoch，报警
        // leader上的epoch应该永远大于等于mds记录的epoch
        LOG(ERROR) << "topoUpdater find copyset("
                   << reportCopySetInfo.GetLogicalPoolId()
                   << "," << reportCopySetInfo.GetId()
                   << "), record epoch:" << recordCopySetInfo.GetEpoch()
                   << " bigger than report epoch:"
                   << reportCopySetInfo.GetEpoch();
        return;
    }

    // 更新到数据库和内存
    if (needUpdate) {
        LOG(INFO) << "topoUpdater find copyset("
                  << reportCopySetInfo.GetLogicalPoolId() << ","
                  << reportCopySetInfo.GetId() << ") need to update";

        int updateCode =
            topo_->UpdateCopySetTopo(reportCopySetInfo);
        if (::curve::mds::topology::kTopoErrCodeSuccess != updateCode) {
            LOG(ERROR) << "topoUpdater update copyset("
                       << reportCopySetInfo.GetLogicalPoolId()
                       << "," << reportCopySetInfo.GetId()
                       << ") got error code: " << updateCode;
            return;
        }
    }
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
