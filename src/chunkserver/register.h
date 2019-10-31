/*
 * Project: curve
 * Created Date: Thur May 9th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_REGISTER_H_
#define SRC_CHUNKSERVER_REGISTER_H_

#include <string>
#include <memory>
#include <vector>
#include "src/fs/local_filesystem.h"
#include "proto/chunkserver.pb.h"

using ::curve::fs::LocalFileSystem;

namespace curve {
namespace chunkserver {
const uint32_t CURRENT_METADATA_VERSION = 0x01;

// register配置选项
struct RegisterOptions {
    std::string mdsListenAddr;
    std::string chunkserverIp;
    int chunkserverPort;
    std::string chunserverStoreUri;
    std::string chunkserverMetaUri;
    std::string chunkserverDiskType;
    int registerRetries;
    int registerTimeout;

    std::shared_ptr<LocalFileSystem> fs;
};

class Register {
 public:
    explicit Register(const RegisterOptions &ops);
    ~Register() {}

    /**
     * @brief RegisterToMDS 向mds注册
     *
     * @param[out] metadata 注册获取的chunkserver元数据信息
     */
    int RegisterToMDS(ChunkServerMetadata *metadata);

    /**
     * @brief 持久化ChunkServer元数据
     *
     * @param[in] metadata
     */
    int PersistChunkServerMeta(const ChunkServerMetadata &metadata);

 private:
    RegisterOptions ops_;

    std::vector<std::string> mdsEps_;
    int inServiceIndex_;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_REGISTER_H_

