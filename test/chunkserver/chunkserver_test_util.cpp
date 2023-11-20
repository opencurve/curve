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
 * Created Date: 18-11-25
 * Author: wudemiao
 */

#include "test/chunkserver/chunkserver_test_util.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <bthread/bthread.h>
#include <butil/endpoint.h>
#include <butil/string_printf.h>
#include <glog/logging.h>
#include <wait.h>

#include <memory>
#include <string>
#include <utility>

#include "src/chunkserver/cli.h"
#include "src/chunkserver/concurrent_apply/concurrent_apply.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/common/concurrent/task_thread_pool.h"
#include "src/common/crc32.h"
#include "src/common/uri_parser.h"
#include "test/chunkserver/fake_datastore.h"

using ::curve::chunkserver::concurrent::ConcurrentApplyOption;
using ::curve::common::UriParser;

namespace curve {
namespace chunkserver {

std::string Exec(const char* cmd) {
    FILE* pipe = popen(cmd, "r");
    if (!pipe) return "ERROR";
    char buffer[4096];
    std::string result = "";
    while (!feof(pipe)) {
        if (fgets(buffer, 1024, pipe) != NULL) result += buffer;
    }
    pclose(pipe);
    return result;
}

std::shared_ptr<FilePool> InitFilePool(
    std::shared_ptr<LocalFileSystem> fsptr,  // NOLINT
    int chunkfileCount, int chunkfileSize, int metaPageSize,
    std::string poolpath, std::string metaPath) {
    auto filePoolPtr = std::make_shared<FilePool>(fsptr);
    if (filePoolPtr == nullptr) {
        LOG(FATAL) << "allocate chunkfile pool failed!";
    }
    int count = 1;
    std::string dirname = poolpath;
    while (count <= chunkfileCount) {
        std::string filename = poolpath + std::to_string(count);
        fsptr->Mkdir(poolpath);
        int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
        char* data = new char[chunkfileSize + 4096];
        memset(data, 'a', chunkfileSize + 4096);
        fsptr->Write(fd, data, 0, chunkfileSize + 4096);
        fsptr->Close(fd);
        count++;
        delete[] data;
    }
    /**
     * Persisting FilePool meta file
     */

    FilePoolMeta meta;
    meta.chunkSize = chunkfileSize;
    meta.metaPageSize = metaPageSize;
    meta.hasBlockSize = true;
    meta.blockSize = 4096;
    meta.filePoolPath = dirname;

    int ret = curve::chunkserver::FilePoolHelper::PersistEnCodeMetaInfo(
        fsptr, meta, metaPath);

    if (ret == -1) {
        LOG(ERROR) << "persist chunkfile pool meta info failed!";
        return nullptr;
    }
    return filePoolPtr;
}

int StartChunkserver(const char* ip, int port, const char* copysetdir,
                     const char* confs, const int snapshotInterval,
                     const int electionTimeoutMs) {
    LOG(INFO) << "Going to start chunk server";

    /* Generally you only need one Server. */
    brpc::Server server;
    butil::EndPoint addr(butil::IP_ANY, port);
    if (0 != CopysetNodeManager::GetInstance().AddService(&server, addr)) {
        LOG(ERROR) << "Fail to add rpc service";
        return -1;
    }
    if (server.Start(port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server, port: " << port
                   << ", errno: " << errno << ", " << strerror(errno);
        return -1;
    }
    LOG(INFO) << "start rpc server success";

    std::shared_ptr<LocalFileSystem> fs(
        LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));  // NOLINT
    const uint32_t kMaxChunkSize = 16 * 1024 * 1024;
    CopysetNodeOptions copysetNodeOptions;
    copysetNodeOptions.ip = ip;
    copysetNodeOptions.port = port;
    copysetNodeOptions.electionTimeoutMs = electionTimeoutMs;
    copysetNodeOptions.snapshotIntervalS = snapshotInterval;
    copysetNodeOptions.electionTimeoutMs = 1500;
    copysetNodeOptions.catchupMargin = 50;
    copysetNodeOptions.chunkDataUri = copysetdir;
    copysetNodeOptions.chunkSnapshotUri = copysetdir;
    copysetNodeOptions.logUri = copysetdir;
    copysetNodeOptions.raftMetaUri = copysetdir;
    copysetNodeOptions.raftSnapshotUri = copysetdir;
    copysetNodeOptions.maxChunkSize = kMaxChunkSize;
    copysetNodeOptions.concurrentapply = new ConcurrentApplyModule();
    copysetNodeOptions.localFileSystem = fs;

    std::string copiedUri(copysetdir);
    std::string chunkDataDir;
    std::string protocol = UriParser::ParseUri(copiedUri, &chunkDataDir);
    if (protocol.empty()) {
        LOG(FATAL) << "not support chunk data uri's protocol"
                   << " error chunkDataDir is: " << chunkDataDir;
    }
    copysetNodeOptions.chunkFilePool = std::make_shared<FakeFilePool>(fs);
    if (nullptr == copysetNodeOptions.chunkFilePool) {
        LOG(FATAL) << "new chunfilepool failed";
    }
    FilePoolOptions cfop;
    if (false == copysetNodeOptions.chunkFilePool->Initialize(cfop)) {
        LOG(FATAL) << "chunfilepool init failed";
    } else {
        LOG(INFO) << "chunfilepool init success";
    }

    ConcurrentApplyOption opt{2, 1, 2, 1};
    LOG_IF(FATAL, false == copysetNodeOptions.concurrentapply->Init(opt))
        << "Failed to init concurrent apply module";

    Configuration conf;
    if (conf.parse_from(confs) != 0) {
        LOG(ERROR) << "Fail to parse configuration `" << confs << '\'';
        return -1;
    }

    std::vector<PeerId> peerIds;
    conf.list_peers(&peerIds);
    std::vector<Peer> peers;
    for (PeerId peerId : peerIds) {
        Peer peer;
        peer.set_address(peerId.to_string());
        peers.push_back(peer);
    }

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    CopysetNodeManager::GetInstance().Init(copysetNodeOptions);
    CopysetNodeManager::GetInstance().Run();
    CHECK(CopysetNodeManager::GetInstance().CreateCopysetNode(
        logicPoolId, copysetId, peers));
    auto copysetNode = CopysetNodeManager::GetInstance().GetCopysetNode(
        logicPoolId, copysetId);
    DataStoreOptions options;
    options.baseDir = "./test-temp";
    options.chunkSize = 16 * 1024 * 1024;
    options.metaPageSize = 4 * 1024;
    options.blockSize = 4 * 1024;
    std::shared_ptr<FakeCSDataStore> dataStore =
        std::make_shared<FakeCSDataStore>(options, fs);
    copysetNode->SetCSDateStore(dataStore);

    LOG(INFO) << "start chunkserver success";
    /* Wait until 'CTRL-C' is pressed. then Stop() and Join() the service */
    server.RunUntilAskedToQuit();
    LOG(INFO) << "server test service is going to quit";

    CopysetNodeManager::GetInstance().DeleteCopysetNode(logicPoolId, copysetId);
    copysetNodeOptions.concurrentapply->Stop();

    return 0;
}

butil::Status WaitLeader(const LogicPoolID& logicPoolId,
                         const CopysetID& copysetId, const Configuration& conf,
                         PeerId* leaderId, int electionTimeoutMs) {
    butil::Status status;
    const int kMaxLoop = (5 * electionTimeoutMs) / 100;
    for (int i = 0; i < kMaxLoop; ++i) {
        status = GetLeader(logicPoolId, copysetId, conf, leaderId);
        if (status.ok()) {
            /**
             * Waiting for flush noop entry
             */
            ::usleep(electionTimeoutMs * 1000);
            return status;
        } else {
            LOG(WARNING) << "Get leader failed, " << status.error_str();
            usleep(100 * 1000);
        }
    }

    status.set_error(-1, "wait get leader failed, retry %d times", kMaxLoop);
    return status;
}

TestCluster::TestCluster(const std::string& clusterName,
                         const LogicPoolID logicPoolID,
                         const CopysetID copysetID,
                         const std::vector<PeerId>& peers)
    : clusterName_(clusterName),
      snapshotIntervalS_(1),
      electionTimeoutMs_(1000),
      catchupMargin_(10) {
    logicPoolID_ = logicPoolID;
    copysetID_ = copysetID;
    for (auto it = peers.begin(); it != peers.end(); ++it) {
        peers_.insert(*it);
        conf_.add_peer(*it);
    }
}

int TestCluster::StartPeer(const PeerId& peerId, const bool empty,
                           bool getChunkFromPool, bool createChunkFilePool) {
    LOG(INFO) << "going start peer: " << peerId.to_string();
    auto it = peersMap_.find(peerId.to_string());
    if (it != peersMap_.end()) {
        LOG(ERROR) << "StartPeer failed. since " << peerId.to_string()
                   << " is exist";
        return -1;
    }

    peers_.insert(peerId);

    std::unique_ptr<PeerNode> peer(new PeerNode());

    peer->peerId = peerId;

    Configuration conf;
    if (!empty) {
        conf = conf_;
    }
    peer->conf = conf;

    CopysetNodeOptions options;
    options.ip = butil::ip2str(peerId.addr.ip).c_str();
    options.port = peerId.addr.port;
    std::string copysetdir = CopysetDirWithProtocol(peerId);
    options.chunkDataUri = copysetdir;
    options.chunkSnapshotUri = copysetdir;
    options.logUri = copysetdir;
    options.raftMetaUri = copysetdir;
    options.raftSnapshotUri = copysetdir;

    options.snapshotIntervalS = snapshotIntervalS_;
    options.electionTimeoutMs = electionTimeoutMs_;
    options.catchupMargin = catchupMargin_;
    peer->options = options;

    pid_t pid = ::fork();
    if (0 > pid) {
        LOG(ERROR) << "start peer fork failed";
        return -1;
    } else if (0 == pid) {
        /*Starting a ChunkServer in a child process*/
        StartPeerNode(peer->options, peer->conf, getChunkFromPool,
                      createChunkFilePool);
        exit(0);
    }

    LOG(INFO) << "Start peer success, pid: " << pid;
    peer->pid = pid;
    peer->state = PeerNodeState::RUNNING;
    peersMap_.insert(std::pair<std::string, std::unique_ptr<PeerNode>>(
        peerId.to_string(), std::move(peer)));
    return 0;
}

int TestCluster::ShutdownPeer(const PeerId& peerId) {
    LOG(INFO) << "going to shutdown peer: " << peerId.to_string();
    auto it = peersMap_.find(peerId.to_string());
    if (it != peersMap_.end()) {
        int waitState;
        if (0 != kill(it->second->pid, SIGINT)) {
            LOG(ERROR) << "Stop peer: " << peerId.to_string() << "failed,"
                       << "errno: " << errno
                       << ", error str: " << strerror(errno);
            return -1;
        }
        waitpid(it->second->pid, &waitState, 0);
        peersMap_.erase(peerId.to_string());
        return 0;
    } else {
        LOG(ERROR) << "Stop peer: " << peerId.to_string() << "failed,"
                   << "since this peer is no exist";
        return -1;
    }
}

int TestCluster::StopPeer(const PeerId& peerId) {
    auto it = peersMap_.find(peerId.to_string());
    if (it != peersMap_.end()) {
        if (it->second->state != PeerNodeState::RUNNING) {
            LOG(WARNING) << "Hang peer: " << peerId.to_string()
                         << " is not running, so cann't stop";
            return -1;
        }
        if (0 != kill(it->second->pid, SIGSTOP)) {
            LOG(ERROR) << "Hang peer: " << peerId.to_string() << "failed,"
                       << "errno: " << errno
                       << ", error str: " << strerror(errno);
            return -1;
        }
        it->second->state = PeerNodeState::STOP;
        return 0;
    } else {
        LOG(ERROR) << "Hang peer: " << peerId.to_string() << " failed,"
                   << " since this peer is no exist";
        return -1;
    }
}

int TestCluster::ContPeer(const PeerId& peerId) {
    auto it = peersMap_.find(peerId.to_string());
    if (it != peersMap_.end()) {
        if (it->second->state != PeerNodeState::STOP) {
            LOG(WARNING) << "peer: " << peerId.to_string()
                         << "is not STOP, so cann't CONT";
            return -1;
        }
        if (0 != kill(it->second->pid, SIGCONT)) {
            LOG(ERROR) << "Cont peer: " << peerId.to_string() << "failed,"
                       << "errno: " << errno
                       << ", error str: " << strerror(errno);
            return -1;
        }
        it->second->state = PeerNodeState::RUNNING;
        return 0;
    } else {
        LOG(ERROR) << "Cont peer: " << peerId.to_string() << "failed,"
                   << "since this peer is no exist";
        return -1;
    }
}

int TestCluster::WaitLeader(PeerId* leaderId) {
    butil::Status status;
    /**
     * Waiting for the election to end
     */
    ::usleep(2 * electionTimeoutMs_ * 1000);
    const int kMaxLoop = (3 * electionTimeoutMs_) / 100;
    for (int i = 0; i < kMaxLoop; ++i) {
        ::usleep(100 * 1000);
        status = GetLeader(logicPoolID_, copysetID_, conf_, leaderId);
        if (status.ok()) {
            /**
             * Due to the need to submit the application noop entry after the
             * election to provide services, So we need to wait for the noop
             * application here. If the wait time is too short, it may be easy
             * to fail, so we need to improve it later
             */
            usleep(electionTimeoutMs_ * 1000);
            LOG(INFO) << "Wait leader success, leader is: "
                      << leaderId->to_string();
            return 0;
        } else {
            LOG(WARNING) << "Get leader failed, error: " << status.error_str()
                         << ", retry " << i + 1 << "th time.";
        }
    }
    return -1;
}

int TestCluster::StopAllPeers() {
    int waitState;
    for (auto it = peersMap_.begin(); it != peersMap_.end(); ++it) {
        kill(it->second->pid, SIGINT);
        waitpid(it->second->pid, &waitState, 0);
    }
    return 0;
}

const Configuration TestCluster::CopysetConf() const { return conf_; }

int TestCluster::SetsnapshotIntervalS(int snapshotIntervalS) {
    snapshotIntervalS_ = snapshotIntervalS;
    return 0;
}

int TestCluster::SetCatchupMargin(int catchupMargin) {
    catchupMargin_ = catchupMargin;
    return 0;
}

int TestCluster::SetElectionTimeoutMs(int electionTimeoutMs) {
    electionTimeoutMs_ = electionTimeoutMs;
    return 0;
}

int TestCluster::StartPeerNode(CopysetNodeOptions options,
                               const Configuration conf,
                               bool enableGetchunkFromPool,
                               bool createChunkFilePool) {
    /**
     * Used for annotation to explain the cmd format
     */
    std::string cmdFormat = R"(
        ./bazel-bin/test/chunkserver/server-test
        -ip=%s
        -port=%d
        -copyset_dir=%s
        -conf=%s
        -election_timeout_ms=%d
        -snapshot_interval_s=%d
        -catchup_margin=%d
        -logic_pool_id=%d
        -copyset_id=%d
        -raft_sync=true
        -enable_getchunk_from_pool=false
        -create_chunkfilepool=true
    )";

    std::string confStr;
    std::vector<PeerId> peers;
    conf.list_peers(&peers);
    for (auto it = peers.begin(); it != peers.end(); ++it) {
        confStr += it->to_string();
        confStr += ",";
    }
    // Remove the last comma
    confStr.pop_back();

    std::string cmd_dir("./bazel-bin/test/chunkserver/server-test");
    std::string cmd("server-test");
    std::string ip;
    butil::string_printf(&ip, "-ip=%s", options.ip.c_str());
    std::string port;
    butil::string_printf(&port, "-port=%d", options.port);
    std::string confs;
    butil::string_printf(&confs, "-conf=%s", confStr.c_str());
    std::string copyset_dir;
    butil::string_printf(&copyset_dir, "-copyset_dir=%s",
                         options.chunkDataUri.c_str());
    std::string election_timeout_ms;
    butil::string_printf(&election_timeout_ms, "-election_timeout_ms=%d",
                         options.electionTimeoutMs);
    std::string snapshot_interval_s;
    butil::string_printf(&snapshot_interval_s, "-snapshot_interval_s=%d",
                         options.snapshotIntervalS);
    std::string catchup_margin;
    butil::string_printf(&catchup_margin, "-catchup_margin=%d",
                         options.catchupMargin);
    std::string getchunk_from_pool;
    butil::string_printf(&getchunk_from_pool, "-enable_getchunk_from_pool=%d",
                         enableGetchunkFromPool);
    std::string create_pool;
    butil::string_printf(&create_pool, "-create_chunkfilepool=%d",
                         createChunkFilePool);
    std::string logic_pool_id;
    butil::string_printf(&logic_pool_id, "-logic_pool_id=%d", logicPoolID_);
    std::string copyset_id;
    butil::string_printf(&copyset_id, "-copyset_id=%d", copysetID_);
    std::string raft_sync;
    butil::string_printf(&raft_sync, "-raft_sync=%s", "true");

    char* arg[] = {const_cast<char*>(cmd.c_str()),
                   const_cast<char*>(ip.c_str()),
                   const_cast<char*>(port.c_str()),
                   const_cast<char*>(confs.c_str()),
                   const_cast<char*>(copyset_dir.c_str()),
                   const_cast<char*>(election_timeout_ms.c_str()),
                   const_cast<char*>(snapshot_interval_s.c_str()),
                   const_cast<char*>(catchup_margin.c_str()),
                   const_cast<char*>(logic_pool_id.c_str()),
                   const_cast<char*>(copyset_id.c_str()),
                   const_cast<char*>(getchunk_from_pool.c_str()),
                   const_cast<char*>(create_pool.c_str()),
                   NULL};

    ::execv(cmd_dir.c_str(), arg);

    return 0;
}

const std::string TestCluster::CopysetDirWithProtocol(const PeerId& peerId) {
    std::string copysetdir;
    butil::string_printf(&copysetdir, "local://./%s-%d-%d",
                         butil::ip2str(peerId.addr.ip).c_str(),
                         peerId.addr.port, 0);
    return copysetdir;
}

const std::string TestCluster::CopysetDirWithoutProtocol(const PeerId& peerId) {
    std::string copysetdir;
    butil::string_printf(&copysetdir, "./%s-%d-%d",
                         butil::ip2str(peerId.addr.ip).c_str(),
                         peerId.addr.port, 0);
    return copysetdir;
}

const std::string TestCluster::RemoveCopysetDirCmd(const PeerId& peerId) {
    std::string cmd;
    butil::string_printf(&cmd, "rm -fr %s-%d-%d",
                         butil::ip2str(peerId.addr.ip).c_str(),
                         peerId.addr.port, 0);
    return cmd;
}

LogicPoolID TestCluster::logicPoolID_ = 0;
CopysetID TestCluster::copysetID_ = 0;

}  // namespace chunkserver
}  // namespace curve
