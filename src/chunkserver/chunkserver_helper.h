/*
 * Project: curve
 * Created Date: Thur May 16th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_CHUNKSERVER_HELPER_H_
#define SRC_CHUNKSERVER_CHUNKSERVER_HELPER_H_

#include <string>
#include "proto/chunkserver.pb.h"

namespace curve {
namespace chunkserver {
class ChunkServerMetaHelper {
 public:
    static bool EncodeChunkServerMeta(
        const ChunkServerMetadata &meta, std::string *out);

    static bool DecodeChunkServerMeta(
        const std::string &meta, ChunkServerMetadata *out);

    static uint32_t MetadataCrc(const ChunkServerMetadata &meta);
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVER_HELPER_H_

