/*
 * Project: curve
 * Created Date: 2019-10-10
 * Author: charisu
 * Copyright (c) 2018 netease
 */
#include "src/tools/snapshot_read.h"

namespace curve {
namespace tool {

int SnapshotRead::Init(const std::string& snapshotId,
                       const std::string& confPath) {
    if (!repo_) {
        std::cout << "repo has hot been inited!" << std::endl;
        return -1;
    }
    if (!s3Adapter_) {
        std::cout << "s3Adapter has hot been inited!" << std::endl;
        return -1;
    }
    Configuration conf;
    conf.SetConfigPath(confPath);
    LOG_IF(FATAL, !conf.LoadConfig()) << "load configuration fail, conf path = "
        << confPath;
    // 从配置文件读取数据库配置信息
    std::string dbName;
    std::string dbUser;
    std::string dbPassword;
    std::string dbUrl;
    int dbPoolSize;
    LOG_IF(FATAL, !conf.GetStringValue("metastore.db_name", &dbName));
    LOG_IF(FATAL, !conf.GetStringValue("metastore.db_user", &dbUser));
    LOG_IF(FATAL, !conf.GetStringValue("metastore.db_address", &dbUrl));
    LOG_IF(FATAL, !conf.GetStringValue("metastore.db_passwd", &dbPassword));
    LOG_IF(FATAL, !conf.GetIntValue("metastore.db_poolsize", &dbPoolSize));

    int ret = repo_->connectDB(dbName, dbUser, dbUrl, dbPassword, dbPoolSize);
    if (ret != OperationOK) {
        std::cout << "connectDB fail with errCode: " << ret << std::endl;
        return -1;
    }
    ret = repo_->useDataBase();
    if (ret != OperationOK) {
        std::cout << "useDataBase fail with errCode: " << ret << std::endl;
        return -1;
    }
    // 从数据库加载快照的文件名和序列号
    ret = repo_->QuerySnapshotRepoItem(snapshotId.c_str(), &item_);
    if (ret != OperationOK) {
        std::cout << "get snapshot item fail with errCode: "
                  << ret << std::endl;
        return -1;
    }
    // 初始化S3
    std::string s3_config_path;
    LOG_IF(FATAL, !conf.GetStringValue("s3.config_path", &s3_config_path));
    s3Adapter_->Init(s3_config_path);
    return 0;
}

int SnapshotRead::InitChunkMap() {
    std::string fileName = item_.fileName;
    uint64_t seqNum = item_.seqNum;
    std::string key = fileName + "-" + std::to_string(seqNum);
    std::string data;
    const Aws::String aws_key(key.c_str(), key.size());
    if ((s3Adapter_->GetObject(aws_key, &data) != 0)) {
        std::cout << "Get Chunk Index Data fail!" << std::endl;
        return -1;
    }
    ChunkMap map;
    if (!map.ParseFromString(data)) {
        std::cout << "Parse chunkMap from string fail!" << std::endl;
        return -1;
    }
    // 解析出seqNum
    for (const auto &m : map.indexmap()) {
        std::string::size_type pos = m.second.find_last_of("-");
        if (std::string::npos == pos) {
            std::cout << "Get seq num fail! name: " << m.second << std::endl;
            return -1;
        }
        std::string seqNumStr = m.second.substr(pos + 1);
        chunkMap_.emplace(m.first, seqNumStr);
    }
    return 0;
}

void SnapshotRead::UnInit() {
    if (s3Adapter_) {
        s3Adapter_->Deinit();
    }
}

void SnapshotRead::GetSnapshotInfo(SnapshotRepoItem *item) {
    *item = item_;
}

int SnapshotRead::Read(char* buf, off_t offset, size_t len) {
    if (buf == nullptr) {
        std::cout << "buf is nullptr!" << std::endl;
        return -1;
    }
    if (chunkMap_.empty()) {
        if (InitChunkMap() != 0) {
            std::cout << "Init chunk map fail!" << std::endl;
            return -1;
        }
    }
    std::string fileName = item_.fileName;
    uint64_t chunkSize = item_.chunkSize;
    // 第一个chunk的index
    uint64_t startchunkindex = offset / chunkSize;
    // 最后一个chunk的index
    uint64_t endchunkindex = (offset + len - 1) / chunkSize;
    // 遍历每一个chunk
    char* tmp = buf;
    for (uint64_t i = startchunkindex; i <= endchunkindex; ++i) {
        // chunk内的偏移
        off_t chunkOffset = 0;
        // chunk内要读的长度
        size_t chunkLen = chunkSize;
        if (i == startchunkindex) {
            chunkOffset = offset % chunkSize;
            chunkLen = std::min((chunkSize - chunkOffset), len);
        } else if (i == endchunkindex) {
            chunkLen = (offset + len - 1) % chunkSize + 1;
        }
        // 判断index是否存在，不存在的话置零
        if (chunkMap_.find(i) == chunkMap_.end()) {
            memset(tmp, 0, chunkLen);
            tmp += chunkLen;
            continue;
        }
        // 获取key
        std::string key = "" + fileName + "-" + std::to_string(i)
                                + "-" + chunkMap_[i];
        if (s3Adapter_->GetObject(key, tmp, chunkOffset, chunkLen) != 0) {
            std::cout << "Read chunk fail!" << std::endl;
            return -1;
        }
        tmp += chunkLen;
    }
    return 0;
}
}  // namespace tool
}  // namespace curve
