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
 * Created Date: Thur May 16th 2019
 * Author: lixiaocui
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

