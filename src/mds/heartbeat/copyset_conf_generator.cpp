/*
 * Project: curve
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <string>
#include "src/mds/heartbeat/copyset_conf_generator.h"

using std::chrono::milliseconds;

namespace curve {
namespace mds {
namespace heartbeat {
bool CopysetConfGenerator::GenCopysetConf(
    ChunkServerIdType reportId,
    const ::curve::mds::topology::CopySetInfo &reportCopySetInfo,
    ::curve::mds::heartbeat::CopySetConf *copysetConf) {
    // topolgy不存在上报的copyset,
    // 发一个空配置指导chunkserver将其删除
    ::curve::mds::topology::CopySetInfo recordCopySetInfo;
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
        ChunkServerIdType candidate =
            LeaderGenCopysetConf(reportCopySetInfo, copysetConf);
        // 有配置下发，把candidate及时更新到topology中
        if (candidate != ::curve::mds::topology::UNINTIALIZE_ID) {
            auto newCopySetInfo = reportCopySetInfo;
            newCopySetInfo.SetCandidate(candidate);
            int updateCode = topo_->UpdateCopySet(newCopySetInfo);
            if (::curve::mds::topology::kTopoErrCodeSuccess != updateCode) {
                // 更新到内存失败
                LOG(ERROR) << "topoUpdater update copySet(logicalPoolId:"
                        << reportCopySetInfo.GetLogicalPoolId()
                        << ", copySetId:" << reportCopySetInfo.GetId()
                        << ") got error code: " << updateCode;
                return false;
            } else {
                // 更新到内存成功
                return true;
            }
        // 没有配置下发
        } else {
            return false;
        }
    } else {
        return FollowerGenCopysetConf(
            reportId, reportCopySetInfo, recordCopySetInfo, copysetConf);
    }
}

ChunkServerIdType CopysetConfGenerator::LeaderGenCopysetConf(
    const ::curve::mds::topology::CopySetInfo &copySetInfo,
    ::curve::mds::heartbeat::CopySetConf *copysetConf) {
    // 转发给调度模块
    return coordinator_->CopySetHeartbeat(copySetInfo, copysetConf);
}

bool CopysetConfGenerator::FollowerGenCopysetConf(ChunkServerIdType reportId,
    const ::curve::mds::topology::CopySetInfo &reportCopySetInfo,
    const ::curve::mds::topology::CopySetInfo &recordCopySetInfo,
    ::curve::mds::heartbeat::CopySetConf *copysetConf) {
    // 如果copyset上面没有candidate, 并且MDS的Epoch>=非leader副本上报上来的Epoch， //NOLINT
    // 可以根据MDS的配置来删除副本

    // mds记录的epoch >= 上报的epoch
    if (recordCopySetInfo.GetEpoch() >= reportCopySetInfo.GetEpoch()) {
        // 延迟清理数据, 避免以下场景发生,copyset的初始配置 ABC, A是leader，epoch=8 //NOLINT
        // 1. mds生成operator, ABC+D, 下发给A
        // 2. A 成功安装快照到 D， 并开复制日志
        // 3. D复制日志的过程中，apply复制组的旧有配置，比如为：epoch=5, ABE
        // 4. 此时mds重启，operator丢失
        // 5. mds启动后首先收到follower-D上报的信息，(ABE, epoch-5), 按照正常的流程，//NOLINT
        //    follower上报的epoch小于mds记录的epoch，mds下发空配置指导D删除copyset, //NOLINT
        //    但此时D已经是复制组的成员了，相当于人为删除了一个副本的数
        // 上述情况可以通过延迟清理避免, 一方面等待一段时间后，leader的心跳会上报D， //NOLINT
        //  D不会被误删。 另一方面，等待一段时间后，日志回放完整，D上报的配置是最新的，//NOLINT
        //  不会被误删。
        steady_clock::duration timePass = steady_clock::now() - mdsStartTime_;
        if (steady_clock::now() - mdsStartTime_ <
                milliseconds(cleanFollowerAfterMs_)) {
            LOG_FIRST_N(INFO, 1) << "begin to clean follower copyset after "
                                 << cleanFollowerAfterMs_ / 1000
                                 << " seconds of mds start";
            return false;
        }
        // 判断该chunkserver是否在配置组中或者是candidate
        bool exist = recordCopySetInfo.HasMember(reportId);
        if (exist || reportId == recordCopySetInfo.GetCandidate()) {
            return false;
        } else {
            LOG(WARNING) << "report chunkserver: " << reportId
                         << " is not a replica or candidate of copyset("
                         << recordCopySetInfo.GetLogicalPoolId()
                         << "," << recordCopySetInfo.GetId() << ")";
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
            auto replica = new ::curve::common::Peer();
            replica->set_id(recordCopySetInfo.GetCandidate());
            replica->set_address(candidate);
            // replica不需要析构，内存由proto接管负责释放
            copysetConf->set_allocated_configchangeitem(replica);
        }

        for (auto peer : recordCopySetInfo.GetCopySetMembers()) {
            std::string addPeer = BuildPeerByChunkserverId(peer);
            if (addPeer.empty()) {
                return false;
            }
            auto replica = copysetConf->add_peers();
            replica->set_id(peer);
            replica->set_address(addPeer);
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
