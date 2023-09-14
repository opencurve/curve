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
 * Created Date: 19-05-27
 * Author: wudemiao
 */

#include "test/integration/common/peer_cluster.h"

#include <wait.h>
#include <brpc/controller.h>
#include <brpc/channel.h>

#include <map>
#include <utility>
#include <algorithm>

#include "src/chunkserver/cli2.h"
#include "src/chunkserver/register.h"
#include "proto/copyset.pb.h"
#include "src/chunkserver/chunkserver_helper.h"
#include "src/fs/fs_common.h"
#include "proto/cli2.pb.h"

namespace curve {
namespace chunkserver {

using curve::fs::FileSystemType;


PeerCluster::PeerCluster(const std::string &clusterName,
                         const LogicPoolID logicPoolID,
                         const CopysetID copysetID,
                         const std::vector<Peer> &peers,
                         std::vector<char **> params,
                         std::map<int, int> paramsIndexs) :
    clusterName_(clusterName),
    snapshotIntervalS_(1),
    electionTimeoutMs_(1000),
    paramsIndexs_(paramsIndexs),
    params_(params),
    isFakeMdsStart_(false) {
    logicPoolID_ = logicPoolID;
    copysetID_ = copysetID;
    for (auto it = peers.begin(); it != peers.end(); ++it) {
        peers_.push_back(*it);
        conf_.add_peer(it->address());
    }
}

int PeerCluster::StartFakeTopoloyService(const std::string &listenAddr) {
    if (isFakeMdsStart_) {
        return 0;
    }
    int ret = fakeMdsServer_.AddService(&fakeTopologyService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE);
    if (ret != 0) {
        LOG(ERROR) << "AddService topology falied";
        return ret;
    }
    ret = fakeMdsServer_.Start(listenAddr.c_str(), nullptr);
    if (ret != 0) {
        LOG(ERROR) << "Start Server failed";
    }
    isFakeMdsStart_ = true;
    return ret;
}

int PeerCluster::StartPeer(const Peer &peer,
                           int id,
                           const bool empty) {
    LOG(INFO) << "going start peer: " << peer.address() << " " << id;
    auto it = peersMap_.find(peer.address());
    if (it != peersMap_.end()) {
        LOG(ERROR) << "StartPeer failed. since " << peer.address()
                   << " is exist";
        return -1;
    }

    std::unique_ptr<PeerNode> peerNode(new PeerNode());

    peerNode->peer = peer;

    Configuration conf;
    if (!empty) {
        conf = conf_;
    }
    peerNode->conf = conf;

    PeerId peerId(peer.address());

    pid_t pid = ::fork();
    if (0 > pid) {
        LOG(ERROR) << "start peer fork failed";
        return -1;
    } else if (0 == pid) {
        /* Starting a ChunkServer in a child process */
        StartPeerNode(id, params_[paramsIndexs_[id]]);
        exit(0);
    }
    LOG(INFO) << "start peer success, peer id = " << pid;
    peerNode->pid = pid;
    peerNode->state = PeerNodeState::RUNNING;
    peersMap_.insert(std::pair<std::string,
                               std::unique_ptr<PeerNode>>(peerId.to_string(),
                                                          std::move(peerNode)));

    // Before creating a copyset, wait for chunkserver to start
    ::usleep(1500 * 1000);

    int ret = CreateCopyset(logicPoolID_, copysetID_, peer, peers_);
    if (0 == ret) {
        LOG(INFO) << "create copyset "
                  << ToGroupIdString(logicPoolID_, copysetID_)
                  << " at peer: " << peer.address() << " success";
    }

    return 0;
}

int PeerCluster::ShutdownPeer(const Peer &peer) {
    PeerId peerId(peer.address());
    LOG(INFO) << "going to shutdown peer: " << peerId.to_string();
    auto it = peersMap_.find(peerId.to_string());
    if (it != peersMap_.end()) {
        int waitState;
        if (0 != kill(it->second->pid, SIGKILL)) {
            LOG(ERROR) << "Stop peer: " << peerId.to_string() << "failed,"
                       << "errno: " << errno << ", error str: "
                       << strerror(errno);
            return -1;
        }
        waitpid(it->second->pid, &waitState, 0);
        LOG(INFO) << "shutdown pid(" << it->second->pid << ") success.";
        peersMap_.erase(peerId.to_string());
        return 0;
    } else {
        LOG(ERROR) << "Stop peer: " << peerId.to_string() << "failed,"
                   << "since this peer is no exist";
        return -1;
    }
}

int PeerCluster::HangPeer(const Peer &peer) {
    LOG(INFO) << "peer cluster: hang " << peer.address();
    PeerId peerId(peer.address());
    auto it = peersMap_.find(peerId.to_string());
    if (it != peersMap_.end()) {
        if (it->second->state != PeerNodeState::RUNNING) {
            LOG(WARNING) << "Hang peer: " << peerId.to_string()
                         << " is not running, so cann't stop";
            return -1;
        }
        if (0 != kill(it->second->pid, SIGSTOP)) {
            LOG(ERROR) << "Hang peer: " << peerId.to_string() << "failed,"
                       << "errno: " << errno << ", error str: "
                       << strerror(errno);
            return -1;
        }
        int waitState;
        waitpid(it->second->pid, &waitState, WUNTRACED);
        LOG(INFO) << "hang pid(" << it->second->pid << ") success.";
        it->second->state = PeerNodeState::STOP;
        return 0;
    } else {
        LOG(ERROR) << "Hang peer: " << peerId.to_string() << " failed,"
                   << " since this peer is no exist";
        return -1;
    }
}

int PeerCluster::SignalPeer(const Peer &peer) {
    LOG(INFO) << "peer cluster: signal " << peer.address();
    PeerId peerId(peer.address());
    auto it = peersMap_.find(peerId.to_string());
    if (it != peersMap_.end()) {
        if (it->second->state != PeerNodeState::STOP) {
            LOG(WARNING) << "peer: " << peerId.to_string()
                         << "is not STOP, so cann't CONT";
            return -1;
        }
        if (0 != kill(it->second->pid, SIGCONT)) {
            LOG(ERROR) << "Cont peer: " << peerId.to_string() << "failed,"
                       << "errno: " << errno << ", error str: "
                       << strerror(errno);
            return -1;
        }
        int waitState;
        waitpid(it->second->pid, &waitState, WCONTINUED);
        LOG(INFO) << "continue pid(" << it->second->pid << ") success.";
        it->second->state = PeerNodeState::RUNNING;
        return 0;
    } else {
        LOG(ERROR) << "Cont peer: " << peerId.to_string() << "failed,"
                   << "since this peer is no exist";
        return -1;
    }
}

int PeerCluster:: ConfirmLeader(const LogicPoolID &logicPoolId,
                        const CopysetID &copysetId,
                        const std::string& leaderAddr,
                        Peer *leader) {
    brpc::Channel channel;
    auto pos = leaderAddr.rfind(":");
    std::string addr = leaderAddr.substr(0, pos);
    if (channel.Init(addr.c_str(), NULL) != 0) {
        LOG(ERROR) <<"Fail to init channel to " << leaderAddr.c_str();
        return -1;
    }
    Peer *peer = new Peer();
    CliService2_Stub stub(&channel);
    GetLeaderRequest2 request;
    GetLeaderResponse2 response;
    brpc::Controller cntl;
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_allocated_peer(peer);
    peer->set_address(addr);

    stub.GetLeader(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) <<"confirm leader fail";
        return -1;
    }
    Peer leader2 = response.leader();
    PeerId leaderId2;
    leaderId2.parse(leader2.address());
    PeerId leaderId1;
    leaderId1.parse(leader->address());
    if (leaderId2.is_empty()) {
        LOG(ERROR) <<"Confirmed leaderId is null";
        return -1;
    }
    if (leaderId2 != leaderId1) {
        LOG(INFO) << "twice leaderId is inconsistent, first is "
                    << leaderId1 << " second is " << leaderId2;
        return -1;
    }
    return 0;
}

int PeerCluster::WaitLeader(Peer *leaderPeer) {
    butil::Status status;
    /**
     * Waiting for the election to end
     */
    ::usleep(3 * electionTimeoutMs_ * 1000);
    const int kMaxLoop = (3 * electionTimeoutMs_) / 100;
    for (int i = 0; i < kMaxLoop; ++i) {
        ::usleep(100 * 1000);
        status = GetLeader(logicPoolID_, copysetID_, conf_, leaderPeer);
        if (status.ok()) {
            /**
             * Due to the need to submit the application noop entry after the election to provide services,
             * So we need to wait for the noop application here. If the wait time is too short, it may be easy to fail, so we need to improve it later
             */
            usleep(electionTimeoutMs_ * 1000);
            LOG(INFO) << "Wait leader success, leader is: "
                      << leaderPeer->address();
            std::string leaderAddr = leaderPeer->address();
            int ret = ConfirmLeader(logicPoolID_, copysetID_,
                                    leaderAddr, leaderPeer);
            if (ret == 0) {
                return ret;
            }
        } else {
            LOG(WARNING) << "Get leader failed, error: " << status.error_str()
                         << ", retry " << i + 1 << "th time.";
        }
    }
    return -1;
}

int PeerCluster::StopAllPeers() {
    int waitState;

    for (auto it = peersMap_.begin(); it != peersMap_.end(); ++it) {
        kill(it->second->pid, SIGKILL);
        waitpid(it->second->pid, &waitState, 0);
    }
    peersMap_.clear();

    return 0;
}

Configuration PeerCluster::CopysetConf() const {
    return conf_;
}

int PeerCluster::SetsnapshotIntervalS(int snapshotIntervalS) {
    snapshotIntervalS_ = snapshotIntervalS;
    return 0;
}

int PeerCluster::SetElectionTimeoutMs(int electionTimeoutMs) {
    electionTimeoutMs_ = electionTimeoutMs;
    return 0;
}

int PeerCluster::StartPeerNode(int id, char *arg[]) {
    struct RegisterOptions opt;
    opt.chunkserverMetaUri = "local://./" + std::to_string(id) +
                             "/chunkserver.dat";
    opt.fs = fs_;
    Register regist(opt);

    ChunkServerMetadata metadata;
    metadata.set_version(CURRENT_METADATA_VERSION);
    metadata.set_id(chunkServerId_++);
    metadata.set_token("tocke-1");
    metadata.set_checksum(ChunkServerMetaHelper::MetadataCrc(metadata));

    CHECK_EQ(0, regist.PersistChunkServerMeta(metadata));

    std::string cmd_dir("./bazel-bin/src/chunkserver/chunkserver");
    ::execv(cmd_dir.c_str(), arg);

    return 0;
}

const std::string PeerCluster::CopysetDirWithProtocol(const Peer &peer) {
    PeerId peerId(peer.address());
    std::string copysetdir;
    butil::string_printf(&copysetdir,
                         "local://./%s-%d-%d",
                         butil::ip2str(peerId.addr.ip).c_str(),
                         peerId.addr.port,
                         0);
    return copysetdir;
}

const std::string PeerCluster::CopysetDirWithoutProtocol(const Peer &peer) {
    PeerId peerId(peer.address());
    std::string copysetdir;
    butil::string_printf(&copysetdir,
                         "./%s-%d-%d",
                         butil::ip2str(peerId.addr.ip).c_str(),
                         peerId.addr.port,
                         0);
    return copysetdir;
}

const std::string PeerCluster::RemoveCopysetDirCmd(const Peer &peer) {
    PeerId peerId(peer.address());
    std::string cmd;
    butil::string_printf(&cmd,
                         "rm -fr %d/copysets", peerId.addr.port);
    return cmd;
}

const std::string PeerCluster::RemoveCopysetLogDirCmd(const Peer &peer,
                                                      LogicPoolID logicPoolID,
                                                      CopysetID copysetID) {
    PeerId peerId(peer.address());
    std::string cmd;
    butil::string_printf(&cmd,
                         "rm -fr %d/copysets/%s",
                         peerId.addr.port,
                         ToGroupIdString(logicPoolID, copysetID).c_str());
    return cmd;
}

int PeerCluster::CreateCopyset(LogicPoolID logicPoolID,
                               CopysetID copysetID,
                               Peer peer,
                               const std::vector<Peer>& peers) {
    LOG(INFO) << "PeerCluster begin create copyset: "
              << ToGroupIdString(logicPoolID, copysetID);

    for (int i = 0; i < 5; ++i) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(5000);

        CopysetRequest request;
        CopysetResponse response;
        request.set_logicpoolid(logicPoolID);
        request.set_copysetid(copysetID);
        for (auto& peer : peers) {
            request.add_peerid(peer.address());
        }

        brpc::Channel channel;
        PeerId peerId(peer.address());
        if (channel.Init(peerId.addr, NULL) != 0) {
            LOG(FATAL) << "Fail to init channel to " << peerId.addr;
        }
        CopysetService_Stub stub(&channel);
        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            LOG(ERROR) << "failed create copsyet, "
                       << cntl.ErrorText() << std::endl;
            ::usleep(1000 * 1000);
            continue;
        }

        if (response.status() == COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS
            || response.status() == COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST) {   //NOLINT
            LOG(INFO) << "create copyset " << ToGroupIdString(logicPoolID,
                                                              copysetID)
                      << " success.";
            return 0;
        }

        ::usleep(1000 * 1000);
    }

    return -1;
}

int PeerCluster::PeerToId(const Peer &peer) {
    PeerId peerId(peer.address());
    return peerId.addr.port;
}

int PeerCluster::GetFollwerPeers(const std::vector<Peer>& peers,
                                 Peer leader,
                                 std::vector<Peer> *followers) {
    for (auto& peer : peers) {
        if (leader.address() != peer.address()) {
            followers->push_back(peer);
        }
    }

    return 0;
}

ChunkServerID PeerCluster::chunkServerId_ = 0;

std::shared_ptr<LocalFileSystem> PeerCluster::fs_
    = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");

/**
 * Normal I/O verification, write it in first, then read it out for verification
 * @param leaderId      Primary ID
 * @param logicPoolId   Logical Pool ID
 * @param copysetId     Copy Group ID
 * @param chunkId   chunk id
 * @param length    The length of each IO
 * @param fillCh    Characters filled in each IO
 * @param loop      The number of times repeatedly initiates IO
 * @param sn        The version number written this time
 */
void WriteThenReadVerify(Peer leaderPeer,
                         LogicPoolID logicPoolId,
                         CopysetID copysetId,
                         ChunkID chunkId,
                         int length,
                         char fillCh,
                         int loop,
                         uint64_t sn) {
    LOG(INFO) << "Write then read verify: " << fillCh;

    PeerId leaderId(leaderPeer.address());
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
    ChunkService_Stub stub(&channel);
    for (int i = 0; i < loop; ++i) {
        // write
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(5000);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(length * i);
            request.set_size(length);
            request.set_sn(sn);
            cntl.request_attachment().resize(length, fillCh);
            stub.WriteChunk(&cntl, &request, &response, nullptr);
            LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                        << cntl.ErrorCode() << " : "
                                        << cntl.ErrorText();
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
        }
        // read
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(5000);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(length * i);
            request.set_size(length);
            request.set_sn(sn);
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                        << cntl.ErrorCode() << " : "
                                        << cntl.ErrorText();
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
            std::string expectRead(length, fillCh);
            ASSERT_STREQ(expectRead.c_str(),
                         cntl.response_attachment().to_string().c_str());
        }
    }
}

/**
 * Normal I/O verification, read data verification
 * @param leaderId      Primary ID
 * @param logicPoolId   Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId   chunk id
 * @param length    The length of each IO
 * @param fillCh    Characters filled in each IO
 * @param loop      The number of times repeatedly initiates IO
 */
void ReadVerify(Peer leaderPeer,
                LogicPoolID logicPoolId,
                CopysetID copysetId,
                ChunkID chunkId,
                int length,
                char fillCh,
                int loop) {
    LOG(INFO) << "Read verify: " << fillCh;
    PeerId leaderId(leaderPeer.address());
    brpc::Channel channel;
    uint64_t sn = 1;
    ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
    ChunkService_Stub stub(&channel);
    for (int i = 0; i < loop; ++i) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(5000);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(length * i);
        request.set_size(length);
        request.set_sn(sn);
        stub.ReadChunk(&cntl, &request, &response, nullptr);
        LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                    << cntl.ErrorCode() << " : "
                                    << cntl.ErrorText();
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  response.status());
        std::string expectRead(length, fillCh);
        ASSERT_STREQ(expectRead.c_str(),
                     cntl.response_attachment().to_string().c_str());
    }
}

/**
 * Verify by reading the snapshot of the chunk
 * @param leaderId Primary ID
 * @param logicPoolId Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId   chunk id
 * @param length    The length of each IO
 * @param fillCh    Characters filled in each IO
 * @param loop      The number of times repeatedly initiates IO
 */
void ReadSnapshotVerify(Peer leaderPeer,
                        LogicPoolID logicPoolId,
                        CopysetID copysetId,
                        ChunkID chunkId,
                        int length,
                        char fillCh,
                        int loop) {
    LOG(INFO) << "Read snapshot verify: " << fillCh;
    PeerId leaderId(leaderPeer.address());
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));

    ChunkService_Stub stub(&channel);

    // Obtain the snapshot version of the chunk
    uint64_t snapSn;
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(5000);
        GetChunkInfoRequest request;
        GetChunkInfoResponse response;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        stub.GetChunkInfo(&cntl, &request, &response, nullptr);
        LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                    << cntl.ErrorCode() << " : "
                                    << cntl.ErrorText();
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  response.status());
        ASSERT_EQ(2, response.chunksn_size());
        snapSn = std::min(response.chunksn(0), response.chunksn(1));
    }

    for (int i = 0; i < loop; ++i) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(5000);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(length*i);
        request.set_size(length);
        request.set_sn(snapSn);
        stub.ReadChunkSnapshot(&cntl, &request, &response, nullptr);
        LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                    << cntl.ErrorCode() << " : "
                                    << cntl.ErrorText();
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  response.status());
        std::string expectRead(length, fillCh);
        ASSERT_STREQ(expectRead.c_str(),
                     cntl.response_attachment().to_string().c_str());
    }
}

/**
 * Delete snapshot of chunk for verification
 * @param leaderId      Primary ID
 * @param logicPoolId   Logical Pool ID
 * @param copysetId     Copy Group ID
 * @param chunkId       chunk id
 * @param csn           corrected sn
 */
void DeleteSnapshotVerify(Peer leaderPeer,
                          LogicPoolID logicPoolId,
                          CopysetID copysetId,
                          ChunkID chunkId,
                          uint64_t csn) {
    LOG(INFO) << "Delete snapshot verify, csn: " << csn;
    PeerId leaderId(leaderPeer.address());
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));

    ChunkService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    ChunkRequest request;
    ChunkResponse response;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_correctedsn(csn);
    stub.DeleteChunkSnapshotOrCorrectSn(&cntl, &request, &response, nullptr);
    LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                << cntl.ErrorCode() << " : "
                                << cntl.ErrorText();
    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
              response.status());
}

/**
 * Abnormal I/O verification, read data does not meet expectations
 * @param leaderId      Primary ID
 * @param logicPoolId   Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId   chunk id
 * @param length    The length of each IO
 * @param fillCh    Characters filled in each IO
 * @param loop      The number of times repeatedly initiates IO
 */
void ReadNotVerify(Peer leaderPeer,
                   LogicPoolID logicPoolId,
                   CopysetID copysetId,
                   ChunkID chunkId,
                   int length,
                   char fillCh,
                   int loop) {
    LOG(INFO) << "Read not verify: " << fillCh;
    PeerId leaderId(leaderPeer.address());
    brpc::Channel channel;
    uint64_t sn = 1;
    ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
    ChunkService_Stub stub(&channel);
    for (int i = 0; i < loop; ++i) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(5000);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(length * i);
        request.set_size(length);
        request.set_sn(sn);
        stub.ReadChunk(&cntl, &request, &response, nullptr);
        LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                    << cntl.ErrorCode() << " : "
                                    << cntl.ErrorText();
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  response.status());
        std::string expectRead(length, fillCh);
        ASSERT_STRNE(expectRead.c_str(),
                     cntl.response_attachment().to_string().c_str());
    }
}

/**
 * Verify availability through read
 * @param leaderId      Primary ID
 * @param logicPoolId   Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId   chunk id
 * @param length    The length of each IO
 * @param fillCh    Characters filled in each IO
 * @param loop      The number of times repeatedly initiates IO
 */
void ReadVerifyNotAvailable(Peer leaderPeer,
                            LogicPoolID logicPoolId,
                            CopysetID copysetId,
                            ChunkID chunkId,
                            int length,
                            char fillCh,
                            int loop) {
    LOG(INFO) << "Read verify not available: " << fillCh;
    PeerId leaderId(leaderPeer.address());
    brpc::Channel channel;
    uint64_t sn = 1;
    ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
    ChunkService_Stub stub(&channel);
    for (int i = 0; i < loop; ++i) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(1000);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(length * i);
        request.set_size(length);
        request.set_sn(sn);
        stub.ReadChunk(&cntl, &request, &response, nullptr);
        LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                    << cntl.ErrorCode() << " : "
                                    << cntl.ErrorText();
        LOG(INFO) << "read: " << CHUNK_OP_STATUS_Name(response.status());
        ASSERT_TRUE(cntl.Failed() ||
            response.status() != CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    }
}

/**
 * Verify availability through write
 * @param leaderId      Primary ID
 * @param logicPoolId   Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId   chunk id
 * @param length    The length of each IO
 * @param fillCh    Characters filled in each IO
 * @param loop      The number of times repeatedly initiates IO
 */
void WriteVerifyNotAvailable(Peer leaderPeer,
                             LogicPoolID logicPoolId,
                             CopysetID copysetId,
                             ChunkID chunkId,
                             int length,
                             char fillCh,
                             int loop) {
    LOG(INFO) << "Write verify not available: " << fillCh;
    PeerId leaderId(leaderPeer.address());
    brpc::Channel channel;
    uint64_t sn = 1;
    ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
    ChunkService_Stub stub(&channel);
    for (int i = 0; i < loop; ++i) {
        // write
        brpc::Controller cntl;
        cntl.set_timeout_ms(1000);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(length * i);
        request.set_size(length);
        request.set_sn(sn);
        cntl.request_attachment().resize(length, fillCh);
        stub.WriteChunk(&cntl, &request, &response, nullptr);
        LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                    << cntl.ErrorCode() << " : "
                                    << cntl.ErrorText();
        ASSERT_TRUE(cntl.Failed() ||
            response.status() != CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    }
}

/**
 * Verify if the copyset status of several replicas is consistent
 * @param peerIds: peers to be verified
 * @param logicPoolID: Logical Pool ID
 * @param copysetId: Copy group ID
 */
void CopysetStatusVerify(const std::vector<Peer> &peers,
                         LogicPoolID logicPoolID,
                         CopysetID copysetId,
                         uint64_t expectEpoch) {
    std::vector<CopysetStatusResponse> resps;
    for (Peer peer : peers) {
        LOG(INFO) << "Get " << peer.address() << " copyset status";
        PeerId peerId(peer.address());
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(peerId.addr, NULL));
        CopysetService_Stub stub(&channel);
        CopysetStatusRequest request;
        CopysetStatusResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(2000);
        request.set_logicpoolid(logicPoolID);
        request.set_copysetid(copysetId);
        Peer *peerP = new Peer();
        request.set_allocated_peer(peerP);
        peerP->set_address(peerId.to_string());
        request.set_queryhash(true);
        stub.GetCopysetStatus(&cntl, &request, &response, nullptr);
        LOG_IF(INFO, cntl.Failed()) << cntl.ErrorText();
        ASSERT_FALSE(cntl.Failed());
        LOG(INFO) << peerId.to_string() << "'s status is: \n"
                  << response.DebugString();
        // The states of multiple replicas are different because there are leaders and followers
        response.clear_state();
        response.clear_peer();
        response.clear_firstindex();
        response.clear_diskindex();
        resps.push_back(response);

        if (0 != expectEpoch) {
            ASSERT_GE(response.epoch(), expectEpoch);
        }
    }

    auto len = resps.size();
    if (len >= 2) {
        for (int i = 1; i < len; ++i) {
            LOG(INFO) << "CopysetStatus " << i + 1 << "th compare.";
            ASSERT_STREQ(resps[0].DebugString().c_str(),
                         resps[i].DebugString().c_str());
        }
    }
}



void TransferLeaderAssertSuccess(PeerCluster *cluster,
                                 const Peer &targetLeader,
                                 braft::cli::CliOptions opt) {
    Peer leaderPeer;
    const int kMaxLoop = 10;
    butil::Status status;
    for (int i = 0; i < kMaxLoop; ++i) {
        status = TransferLeader(cluster->GetLogicPoolId(),
                                cluster->GetCopysetId(),
                                cluster->CopysetConf(),
                                targetLeader,
                                opt);
        if (0 == status.error_code()) {
            cluster->WaitLeader(&leaderPeer);
            if (leaderPeer.address() == targetLeader.address()) {
                break;
            }
        }
        ::sleep(1);
    }
    ASSERT_STREQ(targetLeader.address().c_str(),
                 leaderPeer.address().c_str());
}

}  // namespace chunkserver
}  // namespace curve
