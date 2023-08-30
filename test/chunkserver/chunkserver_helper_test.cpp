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
 * Created Date: Thur May 15th 2019
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include "src/chunkserver/chunkserver_helper.h"
#include "src/chunkserver/register.h"

namespace curve {
namespace chunkserver {
TEST(ChunkServerMeta, test_encode_and_decode) {
    //1 Normal encoding and decoding
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

    //2 Encoding anomaly
    metadata.clear_token();
    strOut.clear();
    ASSERT_FALSE(
        ChunkServerMetaHelper::EncodeChunkServerMeta(metadata, &strOut));

    //3 Decoding exception
    metadata.set_token("hello");
    metadata.set_checksum(9999);
    ASSERT_TRUE(
        ChunkServerMetaHelper::EncodeChunkServerMeta(metadata, &strOut));
    ASSERT_FALSE(
        ChunkServerMetaHelper::DecodeChunkServerMeta(strOut, &metaOut));
}
}  // namespace chunkserver
}  // namespace curve
