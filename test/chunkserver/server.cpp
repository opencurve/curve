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
 * Created Date: 18-8-24
 * Author: wudemiao
 */

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <butil/at_exit.h>
#include <brpc/server.h>

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/chunk_service.h"
#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/concurrent_apply/concurrent_apply.h"
#include "src/chunkserver/datastore/file_pool.h"
#include "src/common/uri_parser.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_storage.h"

using curve::chunkserver::ConcurrentApplyModule;
using curve::chunkserver::Configuration;
using curve::chunkserver::CopysetID;
using curve::chunkserver::CopysetNodeManager;
using curve::chunkserver::CopysetNodeOptions;
using curve::chunkserver::FilePool;
using curve::chunkserver::FilePoolHelper;
using curve::chunkserver::FilePoolOptions;
using curve::chunkserver::LogicPoolID;
using curve::chunkserver::PeerId;
using curve::chunkserver::concurrent::ConcurrentApplyModule;
using curve::chunkserver::concurrent::ConcurrentApplyOption;
using curve::common::Peer;
using curve::common::UriParser;
using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;
using curve::chunkserver::FilePoolHelper;
using curve::chunkserver::FilePoolMeta;

DEFINE_string(ip, "127.0.0.1",
              "Initial configuration of the replication group");
DEFINE_int32(port, 8200, "Listen port of this peer");
DEFINE_string(copyset_dir, "local://./runlog/chunkserver_test0",
              "copyset data dir");
DEFINE_string(conf, "127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0",
              "Initial configuration of the replication group");
DEFINE_int32(election_timeout_ms, 1000, "election timeout");
DEFINE_int32(snapshot_interval_s, 5, "snapshot interval");
DEFINE_int32(catchup_margin, 100, "catchup margin");
DEFINE_int32(logic_pool_id, 2, "logic pool id");
DEFINE_int32(copyset_id, 10001, "copyset id");
DEFINE_bool(enable_getchunk_from_pool, false, "get chunk from pool");
DEFINE_bool(create_chunkfilepool, true, "create chunkfile pool");

butil::AtExitManager atExitManager;

void CreateChunkFilePool(const std::string &dirname, uint64_t chunksize,
                         std::shared_ptr<LocalFileSystem> fsptr) {
    std::string datadir = dirname + "/chunkfilepool";
    std::string metapath = dirname + "/chunkfilepool.meta";

    int count = 1;
    char data[8192];
    memset(data, 0, 8192);
    fsptr->Mkdir(datadir);
    while (count <= 20) {
        std::string filename =
            dirname + "/chunkfilepool/" + std::to_string(count);
        int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
        if (fd < 0) {
            LOG(ERROR) << "Create file failed!";
            continue;
        } else {
            LOG(INFO) << filename.c_str() << " created!";
        }
        for (int i = 0; i <= chunksize / 4096; i++) {
            fsptr->Write(fd, data, i * 4096, 4096);
        }
        fsptr->Close(fd);
        count++;
    }

    FilePoolOptions cpopt;
    cpopt.getFileFromPool = true;
    cpopt.fileSize = chunksize;
    cpopt.metaPageSize = 4096;
    cpopt.metaFileSize = 4096;
    cpopt.blockSize = 4096;

    memcpy(cpopt.filePoolDir, datadir.c_str(), datadir.size());
    memcpy(cpopt.metaPath, metapath.c_str(), metapath.size());

    FilePoolMeta meta;
    meta.chunkSize =  cpopt.fileSize;
    meta.metaPageSize = cpopt.metaFileSize;
    meta.hasBlockSize = true;
    meta.blockSize = cpopt.blockSize;
    meta.filePoolPath = datadir;

    // FIXME(wuhanqing): why void?
    (void)FilePoolHelper::PersistEnCodeMetaInfo(fsptr, meta, metapath);
}

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    /* Generally you only need one Server. */
    brpc::Server server;
    butil::EndPoint addr(butil::IP_ANY, FLAGS_port);
    if (0 != CopysetNodeManager::GetInstance().AddService(&server, addr)) {
        LOG(ERROR) << "Fail to add rpc service";
        return -1;
    }
    curve::chunkserver::RegisterCurveSnapshotStorageOrDie();
    curve::chunkserver::CurveSnapshotStorage::set_server_addr(addr);

    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server: " << errno << ", "
                   << strerror(errno);
        return -1;
    }

    std::shared_ptr<LocalFileSystem> fs(
        LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));
    const uint32_t kMaxChunkSize = 16 * 1024 * 1024;
    // The implementation of TODO(yyk) is not very elegant, and will be refactored in the future
    std::string copysetUri = FLAGS_copyset_dir + "/copysets";
    CopysetNodeOptions copysetNodeOptions;
    copysetNodeOptions.ip = FLAGS_ip;
    copysetNodeOptions.port = FLAGS_port;
    copysetNodeOptions.snapshotIntervalS = FLAGS_snapshot_interval_s;
    copysetNodeOptions.electionTimeoutMs = FLAGS_election_timeout_ms;
    copysetNodeOptions.catchupMargin = FLAGS_catchup_margin;
    copysetNodeOptions.chunkDataUri = copysetUri;
    copysetNodeOptions.chunkSnapshotUri = copysetUri;
    copysetNodeOptions.logUri = copysetUri;
    copysetNodeOptions.raftMetaUri = copysetUri;
    std::string raftSnapshotUri = copysetUri;
    raftSnapshotUri.replace(raftSnapshotUri.find("local"), 5, "curve");
    copysetNodeOptions.raftSnapshotUri = raftSnapshotUri;
    copysetNodeOptions.metaPageSize = 4 * 1024;
    copysetNodeOptions.blockSize = 4 * 1024;
    copysetNodeOptions.maxChunkSize = kMaxChunkSize;

    copysetNodeOptions.concurrentapply = new ConcurrentApplyModule();
    copysetNodeOptions.localFileSystem = fs;

    std::string chunkDataDir;
    std::string protocol =
        UriParser::ParseUri(FLAGS_copyset_dir, &chunkDataDir);
    if (protocol.empty()) {
        LOG(FATAL) << "not support chunk data uri's protocol"
                   << " error chunkDataDir is: " << chunkDataDir;
    }

    copysetNodeOptions.chunkFilePool = std::make_shared<FilePool>(fs);
    if (nullptr == copysetNodeOptions.chunkFilePool) {
        LOG(FATAL) << "new chunfilepool failed";
    }
    FilePoolOptions cfop;
    ::memcpy(cfop.filePoolDir, chunkDataDir.c_str(), chunkDataDir.size());
    cfop.getFileFromPool = FLAGS_enable_getchunk_from_pool;
    cfop.retryTimes = 3;
    cfop.metaPageSize = 4 * 1024;
    cfop.fileSize = kMaxChunkSize;
    if (cfop.getFileFromPool) {
        cfop.metaFileSize = 4096;
        cfop.blockSize = 4096;
        if (FLAGS_create_chunkfilepool) {
            CreateChunkFilePool(chunkDataDir, kMaxChunkSize, fs);
        }
        std::string datadir = chunkDataDir + "/chunkfilepool";
        std::string metapath = chunkDataDir + "/chunkfilepool.meta";
        memcpy(cfop.filePoolDir, datadir.c_str(), datadir.size());
        memcpy(cfop.metaPath, metapath.c_str(), metapath.size());
    }

    if (false == copysetNodeOptions.chunkFilePool->Initialize(cfop)) {
        LOG(FATAL) << "chunfilepool init failed";
    } else {
        LOG(INFO) << "chunfilepool init success";
    }

    ConcurrentApplyOption opt{2, 1, 2, 1};
    LOG_IF(FATAL, false == copysetNodeOptions.concurrentapply->Init(opt))
        << "Failed to init concurrent apply module";

    curve::chunkserver::Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse configuration `" << FLAGS_conf << '\'';
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

    CopysetNodeManager::GetInstance().Init(copysetNodeOptions);
    CopysetNodeManager::GetInstance().Run();
    CopysetNodeManager::GetInstance().CreateCopysetNode(
        FLAGS_logic_pool_id, FLAGS_copyset_id, peers);

    /* Wait until 'CTRL-C' is pressed. then Stop() and Join() the service */
    server.RunUntilAskedToQuit();

    LOG(INFO) << "server test service is going to quit";
    CopysetNodeManager::GetInstance().DeleteCopysetNode(FLAGS_logic_pool_id,
                                                        FLAGS_copyset_id);

    return 0;
}
