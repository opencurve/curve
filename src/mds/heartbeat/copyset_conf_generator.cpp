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
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 */

#include <glog/logging.h>
#include <string>
#include "src/mds/heartbeat/copyset_conf_generator.h"
#include "proto/heartbeat.pb.h"

using std::chrono::milliseconds;
using ::curve::mds::heartbeat::ConfigChangeInfo;
using ::curve::mds::topology::PrintTopoErrCodeDescription;

namespace curve {
namespace mds {
namespace heartbeat {
bool CopysetConfGenerator::GenCopysetConf(
    ChunkServerIdType reportId,
    const ::curve::mds::topology::CopySetInfo &reportCopySetInfo,
    const ::curve::mds::heartbeat::ConfigChangeInfo &configChInfo,
    ::curve::mds::heartbeat::CopySetConf *copysetConf) {
    // reported copyset not exist in topology
    // in this case an empty configuration will be sent to chunkserver
    // to delete it
    ::curve::mds::topology::CopySetInfo recordCopySetInfo;
    if (!topo_->GetCopySet(
        reportCopySetInfo.GetCopySetKey(), &recordCopySetInfo)) {
        LOG(ERROR) << "heartbeatManager receive copyset("
                   << reportCopySetInfo.GetLogicalPoolId()
                   << "," << reportCopySetInfo.GetId()
                   << ") information, but can not get info from topology";
        copysetConf->set_logicalpoolid(reportCopySetInfo.GetLogicalPoolId());
        copysetConf->set_copysetid(reportCopySetInfo.GetId());
        copysetConf->set_epoch(0);
        return true;
    }

    if (reportCopySetInfo.GetLeader() == reportId) {
        ChunkServerIdType candidate =
            LeaderGenCopysetConf(reportCopySetInfo, configChInfo, copysetConf);
        // new config to dispatch available, update candidate to topology
        if (candidate != ::curve::mds::topology::UNINTIALIZE_ID) {
            auto newCopySetInfo = reportCopySetInfo;
            newCopySetInfo.SetCandidate(candidate);
            int updateCode = topo_->UpdateCopySetTopo(newCopySetInfo);
            if (::curve::mds::topology::kTopoErrCodeSuccess != updateCode) {
                // error occurs when update to memory of topology
                LOG(WARNING) << "topoUpdater update copyset("
                        << reportCopySetInfo.GetLogicalPoolId()
                        << "," << reportCopySetInfo.GetId()
                        << ") got error code: "
                        << PrintTopoErrCodeDescription(updateCode);
                return false;
            } else {
                // update to memory successfully
                return true;
            }
        // no config to dispatch
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
    const ::curve::mds::heartbeat::ConfigChangeInfo &configChInfo,
    ::curve::mds::heartbeat::CopySetConf *copysetConf) {
    // pass date to scheduler
    return coordinator_->CopySetHeartbeat(
        copySetInfo, configChInfo, copysetConf);
}

bool CopysetConfGenerator::FollowerGenCopysetConf(ChunkServerIdType reportId,
    const ::curve::mds::topology::CopySetInfo &reportCopySetInfo,
    const ::curve::mds::topology::CopySetInfo &recordCopySetInfo,
    ::curve::mds::heartbeat::CopySetConf *copysetConf) {
    // if there's no candidate on a copyset, and epoch on MDS is larger or equal to what non-leader copy report,                //NOLINT
    // copy(ies) can be deleted according to the configuration of MDS

    // epoch that MDS recorded >= epoch reported
    if (recordCopySetInfo.GetEpoch() >= reportCopySetInfo.GetEpoch()) {
        // delay cleaning is for avoiding cases like the example below:
        // the initial config of a copyset is ABC, in which A is the leader, and epoch = 8                                      //NOLINT
        // 1. MDS gereration an operator ABC+D and dispatch to A
        // 2. A installed snapshot to D successfully, and start copying journal
        // 3. When D is copying the journal, old config of the copyset is applied (e.g. ABE, epoch = 5)                         //NOLINT
        // 4. MDS restart now and operator is lost
        // 5. After the MDS has restarted, it first receive heartbeat from follower-id (ABE, epoch = 5).                        //NOLINT
        //    In normal cases (without delay cleaning), this copyset on D will be deleted since its epoch number is lower,      //NOLINT
        //    but it will be a problem in our case since D is already a member of the copyset, and the deletion of D means      //NOLINT
        //    the elimination of copies.
        // here's how delay cleaning works:
        //    In one way leader will report the existence of D, which promise that D will not be deleted by mistake. In another //NOLINT
        //    way, the configuration that D report will be up-to-date since the journal replay has completed                    //NOLINT

        steady_clock::duration timePass = steady_clock::now() - mdsStartTime_;
        if (steady_clock::now() - mdsStartTime_ <
                milliseconds(cleanFollowerAfterMs_)) {
            LOG_FIRST_N(INFO, 1) << "begin to clean follower copyset after "
                                 << cleanFollowerAfterMs_ / 1000
                                 << " seconds of mds start";
            return false;
        }
        // judge whether the reporting chunkserver is in the copyset it reported, and whether this                             //NOLINT
        // chunkserver is a candidate or going to be added into the copyset
        bool exist = recordCopySetInfo.HasMember(reportId);
        if (exist || reportId == recordCopySetInfo.GetCandidate() ||
            coordinator_->ChunkserverGoingToAdd(
                reportId, reportCopySetInfo.GetCopySetKey())) {
            return false;
        } else {
            LOG(WARNING) << "report chunkserver: " << reportId
                         << " is not a replica or candidate of copyset("
                         << recordCopySetInfo.GetLogicalPoolId()
                         << "," << recordCopySetInfo.GetId() << ")";
        }

        // if the chunkserver doesn't belong to any of the copyset, MDS will dispatch the configuration                        //NOLINT
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
            // memory of replica will be free by proto
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
