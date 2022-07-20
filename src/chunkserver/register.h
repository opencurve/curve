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
 * Created Date: Thur May 9th 2019
 * Author: lixiaocui
 */

#ifndef SRC_CHUNKSERVER_REGISTER_H_
#define SRC_CHUNKSERVER_REGISTER_H_

#include <string>
#include <memory>
#include <vector>
#include "src/fs/local_filesystem.h"
#include "proto/chunkserver.pb.h"
#include "src/chunkserver/epoch_map.h"

using ::curve::fs::LocalFileSystem;

namespace curve {
namespace chunkserver {
const uint32_t CURRENT_METADATA_VERSION = 0x01;

// register配置选项
struct RegisterOptions {
    std::string mdsListenAddr;
    std::string chunkserverInternalIp;
    std::string chunkserverExternalIp;
    bool enableExternalServer;
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
     * @brief RegisterToMDS regist to mds
     *
     * @param[in] localMetadata local meta
     * @param[out] metadata chunkserver meta
     * @param[in,out] epochMap  epochMap to update
     */
    int RegisterToMDS(const ChunkServerMetadata *localMetadata,
        ChunkServerMetadata *metadata,
        const std::shared_ptr<EpochMap> &epochMap);

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

