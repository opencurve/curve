/*
 * Project: curve
 * Created Date: 18-11-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "test/chunkserver/chunkserver_test_util.h"

#include <glog/logging.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>

#include <memory>
#include <string>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"
#include "test/chunkserver/mock_cs_data_store.h"

namespace curve {
namespace chunkserver {

std::string Exec(const char *cmd) {
    FILE *pipe = popen(cmd, "r");
    if (!pipe) return "ERROR";
    char buffer[4096];
    std::string result = "";
    while (!feof(pipe)) {
        if (fgets(buffer, 1024, pipe) != NULL)
            result += buffer;
    }
    pclose(pipe);
    return result;
}

std::shared_ptr<ChunkfilePool> InitChunkfilePool(std::shared_ptr<LocalFileSystem> fsptr,    //NOLINT
                                                 int chunkfileCount,
                                                 int chunkfileSize,
                                                 int metaPageSize,
                                                 std::string poolpath,
                                                 std::string metaPath) {
    auto filePoolPtr = std::make_shared<ChunkfilePool>(fsptr);
    if (filePoolPtr == nullptr) {
        LOG(FATAL) << "allocate chunkfile pool failed!";
    }
    int count = 1;
    std::string dirname = poolpath;
    while (count <= chunkfileCount) {
        std::string  filename = poolpath + std::to_string(count);
        fsptr->Mkdir(poolpath);
        int fd = fsptr->Open(filename.c_str(), O_RDWR | O_CREAT);
        char *data = new char[chunkfileSize + 4096];
        memset(data, 'a', chunkfileSize + 4096);
        fsptr->Write(fd, data, 0, chunkfileSize + 4096);
        fsptr->Close(fd);
        count++;
        delete[] data;
    }
    /**
     * 持久化chunkfilepool meta file
     */
    char persistency[4096] = {0};
    uint32_t chunksize = chunkfileSize;
    uint32_t metapagesize = metaPageSize;
    uint32_t percent = 10;
    ::memcpy(persistency, &chunksize, sizeof(uint32_t));
    ::memcpy(persistency + sizeof(uint32_t), &metapagesize, sizeof(uint32_t));
    ::memcpy(persistency + 2*sizeof(uint32_t), &percent, sizeof(uint32_t));
    ::memcpy(persistency + 3*sizeof(uint32_t), dirname.c_str(), dirname.size());

    int fd = fsptr->Open(metaPath.c_str(), O_RDWR | O_CREAT);
    if (fd < 0) {
        return nullptr;
    }
    int ret = fsptr->Write(fd, persistency, 0, 4096);
    if (ret != 4096) {
        return nullptr;
    }
    fsptr->Close(fd);

    return filePoolPtr;
}

int StartChunkserver(const char *ip,
                     int port,
                     const char *copysetdir,
                     const char *confs,
                     const int snapshotInterval,
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
        LOG(ERROR) << "Fail to start Server, port: " << port << ", errno: "
                   << errno << ", " << strerror(errno);
        return -1;
    }
    LOG(INFO) << "start rpc server success";

    std::shared_ptr<LocalFileSystem> fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
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
    std::string protocol = FsAdaptorUtil::ParserUri(copiedUri, &chunkDataDir);
    if (protocol.empty()) {
        LOG(FATAL) << "not support chunk data uri's protocol"
                   << " error chunkDataDir is: " << chunkDataDir;
    }
    copysetNodeOptions.chunkfilePool = std::make_shared<FakeChunkfilePool>(fs);
    if (nullptr == copysetNodeOptions.chunkfilePool) {
        LOG(FATAL) << "new chunfilepool failed";
    }
    ChunkfilePoolOptions cfop;
    if (false == copysetNodeOptions.chunkfilePool->Initialize(cfop)) {
        LOG(FATAL) << "chunfilepool init failed";
    } else {
        LOG(INFO) << "chunfilepool init success";
    }

    LOG_IF(FATAL, false == copysetNodeOptions.concurrentapply->Init(2, 1))
        << "Failed to init concurrent apply module";

    Configuration conf;
    if (conf.parse_from(confs) != 0) {
        LOG(ERROR) << "Fail to parse configuration `" << confs << '\'';
        return -1;
    }

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    CopysetNodeManager::GetInstance().Init(copysetNodeOptions);
    CHECK(CopysetNodeManager::GetInstance().CreateCopysetNode(logicPoolId,
                                                              copysetId,
                                                              conf));
    auto copysetNode = CopysetNodeManager::GetInstance().GetCopysetNode(
        logicPoolId,
        copysetId);
    DataStoreOptions options;
    options.baseDir = "./test-temp";
    options.chunkSize = 16 * 1024 * 1024;
    options.pageSize = 4 * 1024;
    std::shared_ptr<FakeCSDataStore> dataStore =
        std::make_shared<FakeCSDataStore>(options, fs);
    copysetNode->SetCSDateStore(dataStore);

    LOG(INFO) << "start chunkserver success";
    /* Wait until 'CTRL-C' is pressed. then Stop() and Join() the service */
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }
    LOG(INFO) << "server test service is going to quit";

    CopysetNodeManager::GetInstance().DeleteCopysetNode(logicPoolId, copysetId);
    copysetNodeOptions.concurrentapply->Stop();

    server.Stop(0);
    server.Join();
}

butil::Status WaitLeader(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId,
                         const Configuration &conf,
                         PeerId *leaderId,
                         int electionTimeoutMs) {
    butil::Status status;
    const int kMaxLoop = (5 * electionTimeoutMs) / 100;
    for (int i = 0; i < kMaxLoop; ++i) {
        status = GetLeader(logicPoolId, copysetId, conf, leaderId);
        if (status.ok()) {
            /**
             * 等待 flush noop entry
             */
            ::usleep(electionTimeoutMs * 1000);
            return status;
        } else {
            LOG(WARNING) << "Get leader failed, " << status.error_str();
            usleep(100 * 1000);
        }
    }
}

}  // namespace chunkserver
}  // namespace curve
