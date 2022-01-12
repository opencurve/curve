/*
 *  Copyright (c) 2021 NetEase Inc.
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

#include "curvefs/src/metaserver/copyset/conf_epoch_file.h"

#include <glog/logging.h>
#include <json2pb/json_to_pb.h>
#include <json2pb/pb_to_json.h>
#include <sys/fcntl.h>

#include "absl/cleanup/cleanup.h"
#include "src/common/crc32.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

constexpr uint32_t kConfEpochFileMaxSize = 4096;

int ConfEpochFile::Load(const std::string& path, PoolId* poolID,
                        CopysetId* copysetID, uint64_t* epoch) {
    int fd = fs_->Open(path, O_RDONLY);
    if (fd < 0) {
        LOG(ERROR) << "LoadConfEpoch open file failed: '" << path
                   << "', err: " << errno << ", errmsg: " << strerror(errno);
        return -1;
    }

    auto closeFd = absl::MakeCleanup([fd, this]() { fs_->Close(fd); });

    char json[kConfEpochFileMaxSize] = {0};

    // read data from file
    int size = fs_->Read(fd, json, 0, kConfEpochFileMaxSize);
    if (size <= 0) {
        LOG(ERROR) << "LoadConfEpoch read file failed: '" << path
                   << "', err: " << errno << ", errmsg: " << strerror(errno);
        return -1;
    }

    // parse from read data
    ConfEpoch confEpoch;
    std::string jsonStr(json, size);
    std::string err;
    json2pb::Json2PbOptions opt;
    opt.base64_to_bytes = true;

    if (!json2pb::JsonToProtoMessage(jsonStr, &confEpoch, opt, &err)) {
        LOG(ERROR)
            << "LoadConfEpoch failed to decode conf epoch, encoded data: "
            << jsonStr << ", err: " << err << ", conf file: '" << path << "'";
        return -1;
    }

    // crc checksum
    uint32_t crc32c = ConfEpochCrc(confEpoch);
    if (crc32c != confEpoch.checksum()) {
        LOG(ERROR) << "LoadConfEpoch crc checksum error, encoded data: "
                   << jsonStr << ", expect crc: " << crc32c
                   << ", encoded crc: " << confEpoch.checksum()
                   << ", conf file: '" << path << "'";
        return -1;
    }

    *poolID = confEpoch.poolid();
    *copysetID = confEpoch.copysetid();
    *epoch = confEpoch.epoch();

    LOG(INFO) << "LoadConfEpoch from '" << path
              << "' success, poolId = " << *poolID
              << ", copysetId = " << *copysetID << ", epoch: " << *epoch;
    return 0;
}

int ConfEpochFile::Save(const std::string& path, const PoolId poolID,
                        const CopysetId copysetID, const uint64_t epoch) {
    // convert to protobuf message and calc checksum
    ConfEpoch confEpoch;
    confEpoch.set_poolid(poolID);
    confEpoch.set_copysetid(copysetID);
    confEpoch.set_epoch(epoch);

    uint32_t crc = ConfEpochCrc(confEpoch);
    confEpoch.set_checksum(crc);

    // serialize
    std::string out;
    std::string err;
    json2pb::Pb2JsonOptions opt;
    opt.bytes_to_base64 = true;
    opt.enum_option = json2pb::OUTPUT_ENUM_BY_NUMBER;

    if (!json2pb::ProtoMessageToJson(confEpoch, &out, opt, &err)) {
        LOG(ERROR) << "Failed to encode conf epoch, error: " << err;
        return -1;
    }

    // write to file
    int fd = fs_->Open(path, O_WRONLY | O_CREAT | O_CLOEXEC);
    if (fd < 0) {
        LOG(ERROR) << "Create epoch file '" << path
                   << "' failed, err: " << errno
                   << ", errmsg: " << strerror(errno);
        return -1;
    }

    auto closeFd = absl::MakeCleanup([fd, this]() { fs_->Close(fd); });

    const int size = out.size();
    if (size != fs_->Write(fd, out.c_str(), 0, out.size())) {
        LOG(ERROR) << "SaveConfEpoch write failed, path: '" << path
                   << "', err: " << errno << ", errmsg: " << strerror(errno);
        return -1;
    }

    if (0 != fs_->Fsync(fd)) {
        LOG(ERROR) << "SaveConfEpoch sync failed, path: '" << path
                   << "', err: " << errno << ", errmsg: " << strerror(errno);
        return -1;
    }

    return 0;
}

struct CRC32Helper {
    CRC32Helper() : crc32c(0) {}

    template <typename T>
    CRC32Helper& operator()(const T& val) {
        crc32c = curve::common::CRC32(
            crc32c, reinterpret_cast<const char*>(&val), sizeof(val));

        return *this;
    }

    uint32_t crc32c;
};

uint32_t ConfEpochFile::ConfEpochCrc(const ConfEpoch& confEpoch) {
    const auto poolId = confEpoch.poolid();
    const auto copysetId = confEpoch.copysetid();
    const auto epoch = confEpoch.epoch();
    const uint64_t magic = 0xcfebeefcc32;

    CRC32Helper helper;
    helper(poolId)(copysetId)(epoch)(magic);

    return helper.crc32c;
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
