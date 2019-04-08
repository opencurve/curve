/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/08/30  Wenyu Zhou   Initial version
 */
#include <string>

#include "src/common/configuration.h"

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/heartbeat.h"
#include "src/chunkserver/service_manager.h"
#include "proto/chunkserver.pb.h"
#include "src/fs/local_filesystem.h"
#include "src/fs/fs_common.h"

#ifndef SRC_CHUNKSERVER_CHUNKSERVER_H_
#define SRC_CHUNKSERVER_CHUNKSERVER_H_

const uint32_t CURRENT_METADATA_VERSION = 0x01;

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;

/**
 * ChunkServer服务器管理类和运行入口
 */
class ChunkServer {
 public:
    ChunkServer() {}
    ~ChunkServer() {}

    /**
     * @brief 初始化Chunkserver类以及各个子模块
     * @param[in] argc 命令行参数总数
     * @param[in] argv 命令行参数列表
     * @return 0:成功，非0失败
     */
    int Init(int argc, char** argv);

    /**
     * @brief 清理Chunkserver类以及各个子模块
     * @return 0:成功，非0失败
     */
    int Fini();

    /**
     * @brief 启动Chunkserver类以及各个子模块
     * @return 0:成功，非0失败
     */
    int Run();

    /**
     * @brief 停止Chunkserver类以及各个子模块
     * @return 0:成功，非0失败
     */
    int Stop();

 private:
    /*
     * 初始化配置
     */
    int InitConfig(const std::string& confPath);

    /*
     * 初始化复制组选项
     */
    void InitCopysetNodeOptions();

    /*
     * 初始化QoS子系统选项
     */
    void InitQosOptions();

    /*
     * 初始化静默检查子系统选项
     */
    void InitIntegrityOptions();

    /*
     * 初始化服务子系统选项
     */
    void InitServiceOptions();

    /*
     * 初始化心跳子系统选项
     */
    void InitHeartbeatOptions();

    /*
     * 初始化Chunk文件池选项
     */
    void InitChunkFilePoolOptions();

    /*
     * 解析命令行参数
     */
    int ParseCommandLineFlags(int argc, char** argv);

    /*
     * 相同配置命令行参数比配置文件优先更高
     */
    int ReplaceConfigWithCmdFlags();

    /*
     * 向MDS注册新的ChunkServer
     */
    int RegisterToMetaServer();

    /*
     * 根据持久化数据加载copyset实例
     */
    int ReloadCopysets();

    /*
     * 持久化ChunkServer元数据
     */
    int PersistChunkServerMeta();

    /*
     * 读取ChunkServer元数据
     */
    int ReadChunkServerMeta();

    /*
     * 判定ChunkServer ID是否已经持久化
     */
    bool ChunkServerIdPersisted();

    /*
     * 校验ChunkServer元数据的有效性
     */
    bool ValidateMetaData();

    /*
     * 计算Chunkserver元数据的校验码
     */
    uint32_t MetadataCrc();

 private:
    /*
     * Chunkserver配置模块
     */
    common::Configuration       conf_;

    /*
     * 复制组选项
     */
    CopysetNodeOptions          copysetNodeOptions_;

    /*
     * 复制组管理模块
     */
    CopysetNodeManager          copysetNodeManager_;

    /*
     * 心跳模块选项
     */
    HeartbeatOptions            heartbeatOptions_;

    /*
     * 心跳模块
     */
    Heartbeat                   heartbeat_;

    /*
     * 服务管理模块选项
     */
    ServiceOptions              serviceOptions_;

    /*
     * 服务管理模块
     */
    ServiceManager              serviceManager_;

    /*
     * Chunk文件池选项
     */
    ChunkfilePoolOptions        chunkFilePoolOptions_;

    /*
     * 并发持久化模块
     */
    ConcurrentApplyModule       concurrentapply_;

    /*
     * Chunkserver元数据
     */
    ChunkServerMetadata         metadata_;

    /*
     * Chunkserver数据存储路径
     */
    std::string                 storUri_;

    /*
     * Chunkserver元数据存储路径
     */
    std::string                 metaUri_;

    /*
     * MDS的连接端口
     */
    butil::EndPoint             mdsEp_;

    /*
     * 控制Chunkserver运行或停止
     */
    volatile bool               toStop;

    /*
     * Chunkserver数据存储模块
     */
    std::shared_ptr<LocalFileSystem> fs_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVER_H_
