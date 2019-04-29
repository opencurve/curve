/*
 * Project: curve
 * Created Date: Thur May 15th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include "src/chunkserver/chunkserver_helper.h"
#include "src/chunkserver/register.h"

namespace curve {
namespace chunkserver {
TEST(ChunkServerMeta, test_encode_and_decode) {
    // 1. 正常编解码
    ChunkServerMetadata metadata;
    metadata.set_version(CURRENT_METADATA_VERSION);
    metadata.set_id(1);
    metadata.set_token("hello");
    metadata.set_checksum(ChunkServerMetaHelper::MetadataCrc(metadata));

    std::string strOut;
    ChunkServerMetadata metaOut;
    ASSERT_TRUE(
        ChunkServerMetaHelper::EncodeChunkServerMeta(metadata, &strOut));
    ASSERT_TRUE(ChunkServerMetaHelper::DecodeChunkServerMeta(strOut, &metaOut));
    ASSERT_EQ(metadata.version(), metaOut.version());
    ASSERT_EQ(metadata.id(), metaOut.id());
    ASSERT_EQ(metadata.token(), metaOut.token());

    // 2. 编码异常
    metadata.clear_token();
    strOut.clear();
    ASSERT_FALSE(
        ChunkServerMetaHelper::EncodeChunkServerMeta(metadata, &strOut));

    // 3. 解码异常
    metadata.set_token("hello");
    metadata.set_checksum(9999);
    ASSERT_TRUE(
        ChunkServerMetaHelper::EncodeChunkServerMeta(metadata, &strOut));
    ASSERT_FALSE(
        ChunkServerMetaHelper::DecodeChunkServerMeta(strOut, &metaOut));
}
}  // namespace chunkserver
}  // namespace curve
