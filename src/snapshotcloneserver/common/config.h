/*************************************************************************
 > File Name: config.h
 > Author:
 > Created Time: Wed Nov 21 11:33:46 2018
    > Copyright (c) 2018 netease
 ************************************************************************/

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_CONFIG_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_CONFIG_H_


#include<string>
#include <vector>

namespace curve {
namespace snapshotcloneserver {
// curve client options
struct CurveClientOptions {
    // config path
    std::string configPath;
    // mds root user
    std::string mdsRootUser;
    // mds root password
    std::string mdsRootPassword;
};

// snapshotcloneserver options
struct SnapshotCloneServerOptions {
    // snapshot&clone server address
    std::string  addr;
    // 快照工作线程数
    int snapshotPoolThreadNum;
    // 快照后台线程扫描等待队列和工作队列的扫描周期(单位：ms)
    uint32_t snapshotTaskManagerScanIntervalMs;
    // 转储chunk分片大小
    uint64_t chunkSplitSize;
    // CheckSnapShotStatus调用间隔
    uint32_t checkSnapshotStatusIntervalMs;
    // 最大快照数
    uint32_t maxSnapshotLimit;


    // 克隆恢复工作线程数
    int clonePoolThreadNum;
    // CloneTaskManager 后台线程扫描间隔
    uint32_t cloneTaskManagerScanIntervalMs;
    // clone chunk分片大小
    uint64_t cloneChunkSplitSize;
    // 克隆临时目录
    std::string cloneTempDir;
    // mds root user
    std::string mdsRootUser;
};

// metastore options
struct SnapshotCloneMetaStoreOptions {
    // 数据库名称
    std::string dbName;
    // 数据库用户名
    std::string dbUser;
    // 数据库验证密码
    std::string dbPassword;
    // 数据库服务地址
    std::string dbAddr;
    // 数据库连接池最大连接数
    uint32_t dbPoolSize;
};



}  // namespace snapshotcloneserver
}  // namespace curve
#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_CONFIG_H_
