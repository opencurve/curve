/*
 * Project: curve
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <string>
#include "src/mds/heartbeat/copyset_conf_generator.h"

namespace curve {
namespace mds {
namespace heartbeat {
bool CopysetConfGenerator::GenCopysetConf(
    ChunkServerIdType reportId,
    const CopySetInfo &reportCopySetInfo,
    CopysetConf *copysetConf) {
    // topolgy不存在上报的copyset,
    // 发一个空配置指导chunkserver将其删除
    CopySetInfo recordCopySetInfo;
    if (!topo_->GetCopySet(
        reportCopySetInfo.GetCopySetKey(), &recordCopySetInfo)) {
        LOG(ERROR) << "heartbeatManager receive copySet(logicalPoolId: "
                   << reportCopySetInfo.GetLogicalPoolId()
                   << ", copySetId: " << reportCopySetInfo.GetId()
                   << ") information, but can not get info from topology";
        copysetConf->set_logicalpoolid(reportCopySetInfo.GetLogicalPoolId());
        copysetConf->set_copysetid(reportCopySetInfo.GetId());
        copysetConf->set_epoch(0);
        return true;
    }

    if (reportCopySetInfo.GetLeader() == reportId) {
        return LeaderGenCopysetConf(reportCopySetInfo, copysetConf);
    } else {
        return FollowerGenCopysetConf(
            reportId, reportCopySetInfo, recordCopySetInfo, copysetConf);
    }
}

bool CopysetConfGenerator::LeaderGenCopysetConf(
    const CopySetInfo &copySetInfo, CopysetConf *copysetConf) {
    // 转发给调度模块
    return coordinator_->CopySetHeartbeat(copySetInfo, copysetConf);
}

bool CopysetConfGenerator::FollowerGenCopysetConf(ChunkServerIdType reportId,
    const CopySetInfo &reportCopySetInfo,
    const CopySetInfo &recordCopySetInfo,
    CopysetConf *copysetConf) {
    // 如果copyset上面没有candidate, 并且MDS的Epoch>非leader副本上报上来的Epoch， //NOLINT
    // 可以根据MDS的配置来删除副本。

    // mds记录的epoch大
    if (recordCopySetInfo.GetEpoch() > reportCopySetInfo.GetEpoch()) {
        // 判断该chunkserver是否在配置组中或者是candidate
        bool exist = false;
        for (auto value : recordCopySetInfo.GetCopySetMembers()) {
            if (reportId == value) {
                exist = true;
                break;
            }
        }

        if (!exist && reportId == recordCopySetInfo.GetCandidate()) {
            exist = true;
        }

        // 如果在配置组中或者是candidate，没有conf需要下发
        if (exist) {
            return false;
        }

        // 如果既不在配置组中又不是candidate, 把mds上copyset的配置下发
        copysetConf->set_logicalpoolid(recordCopySetInfo.GetLogicalPoolId());
        copysetConf->set_copysetid(recordCopySetInfo.GetId());
        copysetConf->set_epoch(recordCopySetInfo.GetEpoch());
        if (recordCopySetInfo.HasCandidate()) {
            std::string candidate = BuildPeerByChunkserverId(
                                        recordCopySetInfo.GetCandidate());
            if (candidate.empty()) {
                return false;
            }
            copysetConf->set_configchangeitem(candidate);
        }

        for (auto peer : recordCopySetInfo.GetCopySetMembers()) {
            std::string addPeer = BuildPeerByChunkserverId(peer);
            if (addPeer.empty()) {
                return false;
            }
            copysetConf->add_peers(addPeer);
        }
        return true;
    }
    return false;
}

std::string CopysetConfGenerator::BuildPeerByChunkserverId(
    ChunkServerIdType csId) {
    ChunkServer chunkServer;
    if (!topo_->GetChunkServer(csId, &chunkServer)) {
        return "";
    }

    return ::curve::mds::topology::BuildPeerId(
        chunkServer.GetHostIp(), chunkServer.GetPort(), 0);
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
