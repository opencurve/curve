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

#include <json2pb/pb_to_json.h>
#include <json2pb/json_to_pb.h>
#include <glog/logging.h>

#include "src/common/crc32.h"
#include "src/chunkserver/chunkserver_helper.h"

namespace curve {
namespace chunkserver {
const uint64_t DefaultMagic = 0x6225929368674118;

bool ChunkServerMetaHelper::EncodeChunkServerMeta(
    const ChunkServerMetadata &meta, std::string *out) {
    if (!out->empty()) {
        LOG(ERROR) << "out string must empty!";
        return false;
    }
    std::string err;
    json2pb::Pb2JsonOptions opt;
    opt.bytes_to_base64 = true;
    opt.enum_option = json2pb::OUTPUT_ENUM_BY_NUMBER;

    if (!json2pb::ProtoMessageToJson(meta, out, opt, &err)) {
        LOG(ERROR) << "Failed to encode chunkserver meta data,"
                   << " error: " << err;
        return false;
    }
    return true;
}

bool ChunkServerMetaHelper::DecodeChunkServerMeta(
    const std::string &meta, ChunkServerMetadata *out) {
    std::string jsonStr(meta);
    std::string err;
    json2pb::Json2PbOptions opt;
    opt.base64_to_bytes = true;

    if (!json2pb::JsonToProtoMessage(jsonStr, out, opt, &err)) {
        LOG(ERROR) << "Failed to decode chunkserver meta: " << meta
                   << ", error: " << err.c_str();
        return false;
    }

    // 验证meta是否正确
    uint32_t crc = MetadataCrc(*out);
    if (crc != out->checksum()) {
        LOG(ERROR) << "ChunkServer persisted metadata CRC dismatch."
                   << "current crc: " << crc
                   << ", meta checksum: " << out->checksum();
        return false;
    }

    return true;
}

uint32_t ChunkServerMetaHelper::MetadataCrc(
    const ChunkServerMetadata &meta) {
    uint32_t crc = 0;
    uint32_t ver = meta.version();
    uint32_t id = meta.id();
    const char* token = meta.token().c_str();
    uint64_t magic = DefaultMagic;

    crc = curve::common::CRC32(crc, reinterpret_cast<char*>(&ver), sizeof(ver));
    crc = curve::common::CRC32(crc, reinterpret_cast<char*>(&id), sizeof(id));
    crc = curve::common::CRC32(crc, token, meta.token().size());
    crc = curve::common::CRC32(crc, reinterpret_cast<char*>(&magic),
        sizeof(magic));

    return crc;
}
}  // namespace chunkserver
}  // namespace curve
