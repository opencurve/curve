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
 * Created Date: 18-12-20
 * Author: wudemiao
 */

#include "src/chunkserver/conf_epoch_file.h"

#include <json2pb/pb_to_json.h>
#include <json2pb/json_to_pb.h>

#include "src/common/crc32.h"

namespace curve {
namespace chunkserver {

// conf.epoch文件最大长度
const uint32_t kConfEpochFileMaxSize = 4096;
const uint64_t kConfEpochFileMagic = 0x6225929368674119;

int ConfEpochFile::Load(const std::string &path,
                        LogicPoolID *logicPoolID,
                        CopysetID *copysetID,
                        uint64_t *epoch) {
    int fd = fs_->Open(path.c_str(), O_RDWR);
    if (0 > fd) {
        LOG(ERROR) << "LoadConfEpoch failed open file " << path
                   << ", errno: " << errno
                   << ", error message: " << strerror(errno);
        return -1;
    }

    char json[kConfEpochFileMaxSize] = {0};
    int size = 0;

    // 1. read数据
    size = fs_->Read(fd, json, 0, kConfEpochFileMaxSize);
    if (size <= 0) {
        LOG(ERROR) << "LoadConfEpoch read failed: " << path
                   << ", errno: " << errno
                   << ", error message: " << strerror(errno);
        fs_->Close(fd);
        return -1;
    }
    fs_->Close(fd);

    // 2.反序列化
    ConfEpoch confEpoch;
    std::string jsonStr(json);
    std::string err;
    json2pb::Json2PbOptions opt;
    opt.base64_to_bytes = true;

    if (!json2pb::JsonToProtoMessage(jsonStr, &confEpoch, opt, &err)) {
        LOG(ERROR) << "Failed to decode conf epoch : " << jsonStr
                   << ", error: " << err.c_str();
        return -1;
    }

    // 3. 验证crc
    uint32_t crc32c = ConfEpochCrc(confEpoch);
    if (crc32c != confEpoch.checksum()) {
        LOG(ERROR) << "conf epoch crc error: " << jsonStr;
        return -1;
    }

    *logicPoolID = confEpoch.logicpoolid();
    *copysetID = confEpoch.copysetid();
    *epoch = confEpoch.epoch();

    LOG(INFO) << "Load conf epoch " << path << " success. "
              << "logicPoolID: " << *logicPoolID
              << ", copysetID: " << *copysetID
              << ", epoch: " << *epoch;

    return 0;
}

int ConfEpochFile::Save(const std::string &path,
                        const LogicPoolID logicPoolID,
                        const CopysetID copysetID,
                        const uint64_t epoch) {
    // 1. 转换成conf message
    ConfEpoch confEpoch;
    confEpoch.set_logicpoolid(logicPoolID);
    confEpoch.set_copysetid(copysetID);
    confEpoch.set_epoch(epoch);

    // 计算crc
    uint32_t crc32c = ConfEpochCrc(confEpoch);
    confEpoch.set_checksum(crc32c);

    std::string out;
    std::string err;
    json2pb::Pb2JsonOptions opt;
    opt.bytes_to_base64 = true;
    opt.enum_option = json2pb::OUTPUT_ENUM_BY_NUMBER;

    if (!json2pb::ProtoMessageToJson(confEpoch, &out, opt, &err)) {
        LOG(ERROR) << "Failed to encode conf epoch," << " error: " << err;
        return -1;
    }

    // 2. open文件
    int fd = fs_->Open(path.c_str(), O_RDWR | O_CREAT);
    if (0 > fd) {
        LOG(ERROR) << "LoadConfEpoch failed open file " << path
                   << ", errno: " << errno
                   << ", error message: " << strerror(errno);
        return -1;
    }

    // 3. write文件
    if (out.size() != fs_->Write(fd, out.c_str(), 0, out.size())) {
        LOG(ERROR) << "SaveConfEpoch write failed, path: " << path
                   << ", errno: " << errno
                   << ", error message: " << strerror(errno);
        fs_->Close(fd);
        return -1;
    }

    // 4. 落盘
    if (0 != fs_->Fsync(fd)) {
        LOG(ERROR) << "SaveConfEpoch sync failed, path: " << path
                   << ", errno: " << errno
                   << ", error message: " << strerror(errno);
        fs_->Close(fd);
        return -1;
    }
    fs_->Close(fd);

    return 0;
}

uint32_t ConfEpochFile::ConfEpochCrc(const ConfEpoch &confEpoch) {
    uint32_t crc32c = 0;
    uint32_t logicPoolId    = confEpoch.logicpoolid();
    uint32_t copysetId      = confEpoch.copysetid();
    uint64_t epoch          = confEpoch.epoch();
    uint64_t magic          = kConfEpochFileMagic;

    crc32c = curve::common::CRC32(crc32c,
                                  reinterpret_cast<char *>(&logicPoolId),
                                  sizeof(logicPoolId));
    crc32c = curve::common::CRC32(crc32c,
                                  reinterpret_cast<char *>(&copysetId),
                                  sizeof(copysetId));
    crc32c = curve::common::CRC32(crc32c,
                                  reinterpret_cast<char *>(&epoch),
                                  sizeof(epoch));
    crc32c = curve::common::CRC32(crc32c,
                                  reinterpret_cast<char *>(&magic),
                                  sizeof(magic));

    return crc32c;
}

}  // namespace chunkserver
}  // namespace curve
