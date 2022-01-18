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
 * @Project: curve
 * @Date: 2021-11-11 15:28:57
 * @Author: chenwei
 */

#include "curvefs/src/mds/heartbeat/copyset_conf_generator.h"
#include <glog/logging.h>
#include <string>
#include "curvefs/proto/heartbeat.pb.h"

using std::chrono::milliseconds;
using ::curvefs::mds::heartbeat::ConfigChangeInfo;
using ::curvefs::mds::topology::TopoStatusCode;

namespace curvefs {
namespace mds {
namespace heartbeat {
bool CopysetConfGenerator::GenCopysetConf(
    MetaServerIdType reportId,
    const ::curvefs::mds::topology::CopySetInfo &reportCopySetInfo,
    const ::curvefs::mds::heartbeat::ConfigChangeInfo &configChInfo,
    ::curvefs::mds::heartbeat::CopySetConf *copysetConf) {
    // if copyset is creating return false directly
    if (topo_->IsCopysetCreating(reportCopySetInfo.GetCopySetKey())) {
        return false;
    }

    // reported copyset not exist in topology
    // in this case an empty configuration will be sent to metaserver
    // to delete it
    ::curvefs::mds::topology::CopySetInfo recordCopySetInfo;
    if (!topo_->GetCopySet(reportCopySetInfo.GetCopySetKey(),
                           &recordCopySetInfo)) {
        LOG(ERROR) << "heartbeatManager receive copyset("
                   << reportCopySetInfo.GetPoolId() << ","
                   << reportCopySetInfo.GetId()
                   << ") information, but can not get info from topology";
        copysetConf->set_poolid(reportCopySetInfo.GetPoolId());
        copysetConf->set_copysetid(reportCopySetInfo.GetId());
        copysetConf->set_epoch(0);
        return true;
    }

    if (reportCopySetInfo.GetLeader() == reportId) {
        MetaServerIdType candidate =
            LeaderGenCopysetConf(reportCopySetInfo, configChInfo, copysetConf);
        // new config to dispatch available, update candidate to topology
        if (candidate != ::curvefs::mds::topology::UNINITIALIZE_ID) {
            auto newCopySetInfo = reportCopySetInfo;
            newCopySetInfo.SetCandidate(candidate);
            TopoStatusCode updateCode =
                topo_->UpdateCopySetTopo(newCopySetInfo);
            if (TopoStatusCode::TOPO_OK != updateCode) {
                // error occurs when update to memory of topology
                LOG(WARNING) << "topoUpdater update copyset("
                             << reportCopySetInfo.GetPoolId() << ","
                             << reportCopySetInfo.GetId()
                             << ") got error code: " << updateCode;
                return false;
            } else {
                // update to memory successfully
                return true;
            }
        } else {
            // no config to dispatch
            return false;
        }
    } else {
        return FollowerGenCopysetConf(reportId, reportCopySetInfo,
                                      recordCopySetInfo, copysetConf);
    }
}

MetaServerIdType CopysetConfGenerator::LeaderGenCopysetConf(
    const ::curvefs::mds::topology::CopySetInfo &copySetInfo,
    const ::curvefs::mds::heartbeat::ConfigChangeInfo &configChInfo,
    ::curvefs::mds::heartbeat::CopySetConf *copysetConf) {
    // pass data to scheduler
    return coordinator_->CopySetHeartbeat(copySetInfo, configChInfo,
                                          copysetConf);
}

bool CopysetConfGenerator::FollowerGenCopysetConf(
    MetaServerIdType reportId,
    const ::curvefs::mds::topology::CopySetInfo &reportCopySetInfo,
    const ::curvefs::mds::topology::CopySetInfo &recordCopySetInfo,
    ::curvefs::mds::heartbeat::CopySetConf *copysetConf) {
    // if there's no candidate on a copyset, and epoch on MDS is larger or equal
    // to what non-leader copy report,
    // copy(ies) can be deleted according to the configuration of MDS

    // epoch that MDS recorded >= epoch reported
    if (recordCopySetInfo.GetEpoch() >= reportCopySetInfo.GetEpoch()) {
        steady_clock::duration timePass = steady_clock::now() - mdsStartTime_;
        if (timePass < milliseconds(cleanFollowerAfterMs_)) {
            LOG_FIRST_N(INFO, 1) << "begin to clean follower copyset after "
                                 << cleanFollowerAfterMs_ / 1000
                                 << " seconds of mds start";
            return false;
        }
        // judge whether the reporting metaserver is in the copyset it reported,
        // and whether this metaserver is a candidate or going to be added into
        // the copyset
        bool exist = recordCopySetInfo.HasMember(reportId);
        if (exist || reportId == recordCopySetInfo.GetCandidate() ||
            coordinator_->MetaserverGoingToAdd(
                reportId, reportCopySetInfo.GetCopySetKey())) {
            return false;
        } else {
            LOG(WARNING) << "report metaserver: " << reportId
                         << " is not a replica or candidate of copyset("
                         << recordCopySetInfo.GetPoolId() << ","
                         << recordCopySetInfo.GetId() << ")";
        }

        // if the metaserver doesn't belong to any of the copyset, MDS will
        // dispatch the configuration
        copysetConf->set_poolid(recordCopySetInfo.GetPoolId());
        copysetConf->set_copysetid(recordCopySetInfo.GetId());
        copysetConf->set_epoch(recordCopySetInfo.GetEpoch());
        if (recordCopySetInfo.HasCandidate()) {
            std::string candidateAddr =
                BuildPeerByMetaserverId(recordCopySetInfo.GetCandidate());
            if (candidateAddr.empty()) {
                return false;
            }
            auto replica = new ::curvefs::common::Peer();
            replica->set_id(recordCopySetInfo.GetCandidate());
            replica->set_address(candidateAddr);
            // memory of replica will be free by proto
            copysetConf->set_allocated_configchangeitem(replica);
        }

        for (auto& peer : recordCopySetInfo.GetCopySetMembers()) {
            std::string addPeer = BuildPeerByMetaserverId(peer);
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

std::string CopysetConfGenerator::BuildPeerByMetaserverId(
    MetaServerIdType csId) {
    MetaServer metaServer;
    if (!topo_->GetMetaServer(csId, &metaServer)) {
        return "";
    }

    return ::curvefs::mds::topology::BuildPeerIdWithIpPort(
        metaServer.GetInternalHostIp(), metaServer.GetInternalPort(), 0);
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curvefs
