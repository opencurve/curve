/*
 * Project: curve
 * Created Date: 2019-07-03
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */
#include "src/tools/status_tool.h"
#include <utility>

namespace curve {
namespace tool {

void StatusTool::PrintHelp() {
    std::cout << "Usage : curve_tool command [-confPath=<path>]" << std::endl;
    std::cout << "command : " << std::endl;
    std::cout << "  space : show curve all disk type space, include total space and used space" << std::endl;  //NOLINT
    std::cout << "  status : show curve status, now only chunkserver status" << std::endl;  //NOLINT
    std::cout << "  chunkserver-list : show curve chunkserver-list, list all chunkserver infomation" << std::endl;  //NOLINT
}

int StatusTool::InitMdsRepo(Configuration *conf,
                        std::shared_ptr<curve::mds::MdsRepo> mdsRepo) {
    // 从配置文件读取数据库配置信息
    std::string dbName;
    std::string dbUser;
    std::string dbUrl;
    std::string dbPassword;
    int dbPoolSize;
    LOG_IF(FATAL, !conf->GetStringValue("mds.DbName", &dbName));
    LOG_IF(FATAL, !conf->GetStringValue("mds.DbUser", &dbUser));
    LOG_IF(FATAL, !conf->GetStringValue("mds.DbUrl", &dbUrl));
    LOG_IF(FATAL, !conf->GetStringValue("mds.DbPassword", &dbPassword));
    LOG_IF(FATAL, !conf->GetIntValue("mds.DbPoolSize", &dbPoolSize));

    mdsRepo_ = mdsRepo;

    // init mdsRepo
    if (mdsRepo_->connectDB(dbName, dbUser, dbUrl, dbPassword, dbPoolSize)
                                                    != OperationOK) {
        std::cout << "connectDB fail" << std::endl;
        return -1;
    }

    if (mdsRepo_->createDatabase() != OperationOK) {
        std::cout << "createDatabase fail" << std::endl;
        return -1;
    }

    if (mdsRepo_->useDataBase() != OperationOK) {
        std::cout << "useDataBase fail" << std::endl;
        return -1;
    }

    if (mdsRepo_->createAllTables() != OperationOK) {
        std::cout << "createAllTables fail" << std::endl;
        return -1;
    }

    return 0;
}

int StatusTool::SpaceCmd() {
    std::vector<ChunkServerRepoItem> chunkServerRepoList;
    auto ret = mdsRepo_->LoadChunkServerRepoItems(&chunkServerRepoList);
    if (ret != curve::repo::OperationOK) {
        std::cout << "load chunk server repo fail." << std::endl;
        return -1;
    }

    typedef struct SpaceInfo {
        int64_t totalSize;
        int64_t usedSize;
    } SpaceInfo;

    std::map<std::string, SpaceInfo> spaceMap;

    for (auto rp : chunkServerRepoList) {
        auto it = spaceMap.find(rp.diskType);
        if (it == spaceMap.end()) {
            SpaceInfo spaceInfo = {rp.capacity, rp.used};
            spaceMap.insert(
                std::pair<std::string, SpaceInfo>(rp.diskType, spaceInfo));
        } else {
            it->second.totalSize += rp.capacity;
            it->second.usedSize += rp.used;
        }
    }

    std::cout << "curve space: " << std::endl;

    for (auto it = spaceMap.begin(); it != spaceMap.end(); it++) {
        std::cout << it->first
                  << ": total size = "
                  << it->second.totalSize / mds::kGB << " GB"
                  << ", used size = "
                  << it->second.usedSize / mds::kGB << " GB"
                  << std::endl;
    }

    return 0;
}

int StatusTool::StatusCmd() {
    std::vector<ChunkServerRepoItem> chunkServerRepoList;
    auto ret = mdsRepo_->LoadChunkServerRepoItems(&chunkServerRepoList);
    if (ret != curve::repo::OperationOK) {
        std::cout << "load chunk server repo fail." << std::endl;
        return -1;
    }

    uint64_t totalChunkServerNum = 0;
    uint64_t onlineChunkServerNum = 0;
    uint64_t offlineChunkServerNum = 0;
    for (ChunkServerRepoItem &rp : chunkServerRepoList) {
        totalChunkServerNum++;
        if (rp.onlineState == OnlineState::ONLINE) {
            onlineChunkServerNum++;
        }
        if (rp.onlineState == OnlineState::OFFLINE) {
            offlineChunkServerNum++;
        }
    }

    std::cout << "curve status: " << std::endl;
    std::cout << "chunkserver: total num = " << totalChunkServerNum
            << ", online = " << onlineChunkServerNum
            << ", offline = " << offlineChunkServerNum
            << std::endl;

    return 0;
}

int StatusTool::ChunkServerCmd() {
    std::vector<ChunkServerRepoItem> chunkServerRepoList;
    auto ret = mdsRepo_->LoadChunkServerRepoItems(&chunkServerRepoList);
    if (ret != curve::repo::OperationOK) {
        std::cout << "load chunk server repo fail." << std::endl;
        return -1;
    }

    std::cout << "curve chunkserver list: " << std::endl;

    for (ChunkServerRepoItem &rp : chunkServerRepoList) {
        std::cout << "chunkServerID = " << rp.chunkServerID
                  << ", token = " << rp.token
                  << ", diskType = " << rp.diskType
                  << ", internalHostIP = " << rp.internalHostIP
                  << ", port = " << rp.port
                  << ", serverID = " << rp.serverID
                  << ", rwstatus = "
                  << ChunkServerStatus_Name(static_cast<ChunkServerStatus>
                                                            (rp.rwstatus))
                  << ", diskState = "
                  << DiskState_Name(static_cast<DiskState>(rp.diskState))
                  << ", onlineState = "
                  << OnlineState_Name(static_cast<OnlineState>(rp.onlineState))
                  << ", mountPoint = " << rp.mountPoint
                  << ", capacity = " << rp.capacity / curve::mds::kGB << " GB"
                  << ", used = " << rp.used / curve::mds::kGB << " GB"
                  << std::endl;
    }

    return 0;
}

int StatusTool::RunCommand(const std::string &cmd) {
    if (cmd == "space") {
        return SpaceCmd();
    } else if (cmd == "status") {
        return StatusCmd();
    } else if (cmd == "chunkserver-list") {
        return ChunkServerCmd();
    } else {
        PrintHelp();
    }

    return 0;
}
}  // namespace tool
}  // namespace curve
