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
 * Created Date: 18-8-27
 * Author: wudemiao
 */

#include <vector>
#include "src/tools/curve_cli.h"
#include "src/tools/common.h"

DEFINE_int32(timeout_ms,
             -1, "Timeout (in milliseconds) of the operation");
DEFINE_int32(max_retry,
             3, "Max retry times of each operation");
DEFINE_string(conf,
              "127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0",
              "Initial configuration of the replication group");
DEFINE_string(peer,
              "", "Id of the operating peer");
DEFINE_string(new_conf,
              "", "new conf to reset peer");

DEFINE_bool(affirm, true,
            "If true, command line interactive affirmation is required."
            " Only set false in unit test");
DECLARE_string(mdsAddr);

namespace curve {
namespace tool {
#define CHECK_FLAG(flagname)                                            \
    do {                                                                \
        if ((FLAGS_ ## flagname).empty()) {                             \
            std::cout << __FUNCTION__ << " requires --" # flagname      \
                      << std::endl;                                     \
            return -1;                                                  \
        }                                                               \
    } while (0);                                                        \


bool CurveCli::SupportCommand(const std::string& command) {
    return  (command == kResetPeerCmd || command == kRemovePeerCmd
                                      || command == kTransferLeaderCmd
                                      || command == kDoSnapshot);
}

int CurveCli::Init() {
    return mdsClient_->Init(FLAGS_mdsAddr);
}

int CurveCli::RemovePeer() {
    CHECK_FLAG(conf);
    CHECK_FLAG(peer);

    braft::Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        std::cout << "Fail to parse --conf" << std::endl;
        return -1;
    }
    braft::PeerId removingPeerId;
    if (removingPeerId.parse(FLAGS_peer) != 0) {
        std::cout << "Fail to parse --peer" << std::endl;
        return -1;
    }
    curve::common::Peer removingPeer;
    removingPeer.set_address(removingPeerId.to_string());
    braft::cli::CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = curve::chunkserver::RemovePeer(
                                FLAGS_logicalPoolId,
                                FLAGS_copysetId,
                                conf,
                                removingPeer,
                                opt);
    if (!st.ok()) {
        std::cout << "Remove peer " << removingPeerId << " from copyset "
                  << "(" << FLAGS_logicalPoolId << ", "
                  << FLAGS_copysetId << ")"
                  << " fail, original conf: " << conf
                  << ", detail: " << st << std::endl;
        return -1;
    }
    std::cout << "Remove peer " << removingPeerId << " from copyset "
              << "(" << FLAGS_logicalPoolId << ", " << FLAGS_copysetId << ")"
              << " success, original conf: " << conf << std::endl;
    return 0;
}

int CurveCli::TransferLeader() {
    CHECK_FLAG(conf);
    CHECK_FLAG(peer);

    braft::Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        std::cout << "Fail to parse --conf" << std::endl;
        return -1;
    }
    braft::PeerId targetPeerId;
    if (targetPeerId.parse(FLAGS_peer) != 0) {
        std::cout << "Fail to parse --peer" << std::endl;
        return -1;
    }
    curve::common::Peer targetPeer;
    targetPeer.set_address(targetPeerId.to_string());
    braft::cli::CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = curve::chunkserver::TransferLeader(
                                    FLAGS_logicalPoolId,
                                    FLAGS_copysetId,
                                    conf,
                                    targetPeer,
                                    opt);
    if (!st.ok()) {
        std::cout << "Transfer leader of copyset "
                  << "(" << FLAGS_logicalPoolId << ", "
                  << FLAGS_copysetId << ")"
                  << " to " << targetPeerId
                  << " fail, original conf: " << conf
                  << ", detail: " << st << std::endl;
        return -1;
    }
    std::cout << "Transfer leader of copyset "
                  << "(" << FLAGS_logicalPoolId << ", "
                  << FLAGS_copysetId << ")"
                  << " to " << targetPeerId
                  << " success, original conf: " << conf << std::endl;
    return 0;
}

int CurveCli::ResetPeer() {
    CHECK_FLAG(new_conf);
    CHECK_FLAG(peer);

    if (FLAGS_affirm) {
        std::cout << "Before reset peer, please assure that\n"
                  << "1、Two peers of the copyset is down (use check-copyset)\n"
                  << "2、The alive peer has the newest data\n"
                  << "3、Two down peers could not be rcovered\n"
                  << "If you confirm it, please input:\n"
                  << "Yes, I do!" << std::endl;
        std::string str;
        std::getline(std::cin, str);
        if (str != "Yes, I do!") {
            std::cout << "Reset peer canceled" << std::endl;
            return 0;
        }
    }

    braft::Configuration newConf;
    if (newConf.parse_from(FLAGS_new_conf) != 0) {
        std::cout << "Fail to parse --new_conf" << std::endl;
        return -1;
    }
    braft::PeerId requestPeerId;
    if (requestPeerId.parse(FLAGS_peer) != 0) {
        std::cout << "Fail to parse --peer" << std::endl;
        return -1;
    }
    curve::common::Peer requestPeer;
    requestPeer.set_address(requestPeerId.to_string());
    // 目前reset peer只支持reset为1一个副本，不支持增加副本，
    // 因为不能通过工具在chunkserver上创建copyset
    if (newConf.size() != 1) {
        std::cout << "New conf can only specify one peer!" << std::endl;
        return -1;
    }
    // 新的配置必须包含发送RPC的peer
    if (*newConf.begin() != requestPeerId) {
        std::cout << "New conf must include the target peer!" << std::endl;
        return -1;
    }
    braft::cli::CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;

    butil::Status st = curve::chunkserver::ResetPeer(
                                FLAGS_logicalPoolId,
                                FLAGS_copysetId,
                                newConf,
                                requestPeer,
                                opt);
    if (!st.ok()) {
        std::cout << "Reset peer of copyset "
                  << "(" << FLAGS_logicalPoolId << ", "
                  << FLAGS_copysetId << ")"
                  << " to " << newConf
                  << " fail, requestPeer: " << requestPeerId
                  << ", detail: " << st << std::endl;
        return -1;
    }
    std::cout << "Reset peer of copyset "
              << "(" << FLAGS_logicalPoolId << ", "
              << FLAGS_copysetId << ")"
              << " to " << newConf
              << " success, requestPeer: " << requestPeerId << std::endl;
    return 0;
}

int CurveCli::DoSnapshot() {
    CHECK_FLAG(peer);
    braft::PeerId requestPeerId;
    if (requestPeerId.parse(FLAGS_peer) != 0) {
        std::cout << "Fail to parse --peer" << std::endl;
        return -1;
    }
    curve::common::Peer requestPeer;
    requestPeer.set_address(requestPeerId.to_string());
    return DoSnapshot(FLAGS_logicalPoolId, FLAGS_copysetId, requestPeer);
}

int CurveCli::DoSnapshot(uint32_t lgPoolId, uint32_t copysetId,
                         const curve::common::Peer& peer) {
    braft::cli::CliOptions opt;
    opt.timeout_ms = FLAGS_timeout_ms;
    opt.max_retry = FLAGS_max_retry;
    butil::Status st = curve::chunkserver::Snapshot(
                                FLAGS_logicalPoolId,
                                FLAGS_copysetId,
                                peer,
                                opt);
    if (!st.ok()) {
        std::cout << "Do snapshot of copyset "
                  << "(" << FLAGS_logicalPoolId << ", "
                  << FLAGS_copysetId << ")"
                  << " fail, requestPeer: " << peer.address()
                  << ", detail: " << st << std::endl;
        return -1;
    }
    return 0;
}

int CurveCli::DoSnapshotAll() {
    std::vector<ChunkServerInfo> chunkservers;
    int res = mdsClient_->ListChunkServersInCluster(&chunkservers);
    if (res != 0) {
        std::cout << "ListChunkServersInCluster fail!" << std::endl;
        return -1;
    }
    for (const auto& chunkserver : chunkservers) {
        braft::cli::CliOptions opt;
        opt.timeout_ms = FLAGS_timeout_ms;
        opt.max_retry = FLAGS_max_retry;
        std::string csAddr = chunkserver.hostip() + ":" +
                                std::to_string(chunkserver.port());
        curve::common::Peer peer;
        peer.set_address(csAddr);
        butil::Status st = curve::chunkserver::SnapshotAll(peer, opt);
        if (!st.ok()) {
            std::cout << "Do all snapshot of chunkserver " << csAddr
                      << " fail, error: " << st.error_str() << std::endl;
            res = -1;
        }
    }
    return res;
}

void CurveCli::PrintHelp(const std::string &cmd) {
    std::cout << "Example " << std::endl;
    if (cmd == kResetPeerCmd) {
        std::cout << "curve_ops_tool " << cmd << " -logicalPoolId=1 -copysetId=10001 -peer=127.0.0.1:8080:0 "  // NOLINT
        "-new_conf=127.0.0.1:8080:0 -max_retry=3 -timeout_ms=100" << std::endl;  // NOLINT
    } else if (cmd == kRemovePeerCmd || cmd == kTransferLeaderCmd) {
        std::cout << "curve_ops_tool " << cmd << " -logicalPoolId=1 -copysetId=10001 -peer=127.0.0.1:8080:0 "  // NOLINT
        "-conf=127.0.0.1:8080:0,127.0.0.1:8081:0,127.0.0.1:8082:0 -max_retry=3 -timeout_ms=100" << std::endl;  // NOLINT
    } else if (cmd == kDoSnapshot) {
        std::cout << "curve_ops_tool " << cmd << " -logicalPoolId=1 -copysetId=10001 -peer=127.0.0.1:8080:0 "  // NOLINT
        "-max_retry=3 -timeout_ms=100" << std::endl;
    } else if (cmd == kDoSnapshotAll) {
        std::cout << "curve_ops_tool " << cmd << std::endl;
    } else {
        std::cout << "Command not supported!" << std::endl;
    }
}

int CurveCli::RunCommand(const std::string &cmd) {
    if (Init() != 0) {
        std::cout << "Init CurveCli tool failed" << std::endl;
        return -1;
    }
    if (cmd == kRemovePeerCmd) {
        return RemovePeer();
    }
    if (cmd == kTransferLeaderCmd) {
        return TransferLeader();
    }
    if (cmd == kResetPeerCmd) {
        return ResetPeer();
    }
    if (cmd == kDoSnapshot) {
        return DoSnapshot();
    }
    if (cmd == kDoSnapshotAll) {
        return DoSnapshotAll();
    }
    std::cout << "Command not supported!" << std::endl;
    return -1;
}
}  // namespace tool
}  // namespace curve

