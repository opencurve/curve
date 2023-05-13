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
 * Created Date: 2019-12-03
 * Author: lixiaocui
 */

#include <butil/endpoint.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <string>
#include "src/chunkserver/heartbeat_helper.h"
#include "include/chunkserver/chunkserver_common.h"
#include "proto/chunkserver.pb.h"

namespace curve {
namespace chunkserver {
bool HeartbeatHelper::BuildNewPeers(
    const CopySetConf &conf, std::vector<Peer> *newPeers) {
    // 检验目标节点和待删除节点是否有效
    std::string target(conf.configchangeitem().address());
    std::string old(conf.oldpeer().address());
    if (!PeerVaild(target) || !PeerVaild(old)) {
        return false;
    }

    // 生成newPeers
    for (int i = 0; i < conf.peers_size(); i++) {
        std::string peer = conf.peers(i).address();
        // 检验conf中的peer是否有效
        if (!PeerVaild(peer)) {
            return false;
        }

        // newPeers中不包含old副本
        if (conf.peers(i).address() != old) {
            newPeers->emplace_back(conf.peers(i));
        }
    }

    newPeers->emplace_back(conf.configchangeitem());
    return true;
}

bool HeartbeatHelper::PeerVaild(const std::string &peer) {
    PeerId peerId;
    return 0 == peerId.parse(peer);
}

bool HeartbeatHelper::CopySetConfValid(
    const CopySetConf &conf, const CopysetNodePtr &copyset) {
    // chunkserver中不存在需要变更的copyset, 报警
    if (copyset == nullptr) {
        LOG(ERROR) << "Failed to find copyset(" << conf.logicalpoolid()
            << "," <<  conf.copysetid() << "), groupId: "
            << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
        return false;
    }

    // 下发的变更epoch < copyset实际的epoch，报错
    if (conf.epoch() < copyset->GetConfEpoch()) {
        LOG(WARNING) << "Config change epoch:" << conf.epoch()
                << " is smaller than current:" << copyset->GetConfEpoch()
                << " on copyset("
                << conf.logicalpoolid() << "," <<  conf.copysetid()
                << "), groupId: "
                << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid())
                << ", refuse change";
        return false;
    }

    return true;
}

bool HeartbeatHelper::NeedPurge(const butil::EndPoint &csEp,
    const CopySetConf &conf, const CopysetNodePtr &copyset) {
    (void)copyset;
    // CLDCFS-1004 bug-fix: mds下发epoch为0, 配置为空的copyset
    if (0 == conf.epoch() && conf.peers().empty()) {
        LOG(INFO) << "Clean copyset "
            << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid())
            << "in peer " << csEp
            << ", witch is not exist in mds record";
        return true;
    }

    // 该chunkserrver不在copyset的配置中,需要清理
    std::string chunkserverEp = std::string(butil::endpoint2str(csEp).c_str());
    for (int i = 0; i < conf.peers_size(); i++) {
        if (conf.peers(i).address().find(chunkserverEp) != std::string::npos) {
            return false;
        }
    }
    return true;
}

bool HeartbeatHelper::ChunkServerLoadCopySetFin(const std::string peerId) {
    if (!PeerVaild(peerId)) {
        LOG(WARNING) << peerId << " is invalid";
        return false;
    }

    PeerId peer;
    peer.parse(peerId);
    const char *ip = butil::ip2str(peer.addr.ip).c_str();
    int port = peer.addr.port;
    brpc::Channel channel;
    if (channel.Init(ip, port, NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to ip:" << ip << " port:" << port;
        return false;
    }
    ChunkServerService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(500);
    ChunkServerStatusRequest req;
    ChunkServerStatusResponse rep;
    stub.ChunkServerStatus(&cntl, &req, &rep, nullptr);
    if (cntl.Failed()) {
        LOG(WARNING) << "Send ChunkServerStatusRequest failed, cntl.errorText ="
            << cntl.ErrorText();
        return false;
    }

    return rep.copysetloadfin();
}

}  // namespace chunkserver
}  // namespace curve

