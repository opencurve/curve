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

#include <json2pb/pb_to_json.h>
#include <json2pb/json_to_pb.h>
#include <brpc/channel.h>
#include <fcntl.h>

#include <string>
#include <vector>

#include "src/common/crc32.h"
#include "src/common/string_util.h"
#include "src/chunkserver/register.h"
#include "src/chunkserver/uri_paser.h"
#include "src/chunkserver/chunkserver_helper.h"
#include "proto/topology.pb.h"

namespace curve {
namespace chunkserver {
Register::Register(const RegisterOptions &ops) {
    this->ops_ = ops;

    // 解析mds的多个地址
    ::curve::common::SplitString(ops.mdsListenAddr, ",", &mdsEps_);
    // 检验每个地址的合法性
    for (auto addr : mdsEps_) {
        butil::EndPoint endpt;
        if (butil::str2endpoint(addr.c_str(), &endpt) < 0) {
            LOG(FATAL) << "Invalid sub mds ip:port provided: " << addr;
        }
    }
    inServiceIndex_ = 0;
}

int Register::RegisterToMDS(ChunkServerMetadata *metadata) {
    ::curve::mds::topology::ChunkServerRegistRequest req;
    ::curve::mds::topology::ChunkServerRegistResponse resp;

    req.set_disktype(ops_.chunkserverDiskType);
    req.set_diskpath(ops_.chunserverStoreUri);
    req.set_hostip(ops_.chunkserverIp);
    req.set_port(ops_.chunkserverPort);

    LOG(INFO) << ops_.chunkserverIp << ":" << ops_.chunkserverPort
              <<" Registering to MDS " << mdsEps_[inServiceIndex_];
    int retries = ops_.registerRetries;
    while (retries >= 0) {
        brpc::Channel channel;
        brpc::Controller cntl;

        cntl.set_timeout_ms(ops_.registerTimeout);

        if (channel.Init(mdsEps_[inServiceIndex_].c_str(), NULL) != 0) {
            LOG(ERROR) << ops_.chunkserverIp << ":" << ops_.chunkserverPort
                       << " Fail to init channel to MDS "
                       << mdsEps_[inServiceIndex_];
            return -1;
        }
        curve::mds::topology::TopologyService_Stub stub(&channel);

        stub.RegistChunkServer(&cntl, &req, &resp, nullptr);
        // TODO(lixiaocui): 后续错误码和mds共享后改成枚举类型
        if (!cntl.Failed() && resp.statuscode() == 0) {
            break;
        } else {
            LOG(ERROR) << ops_.chunkserverIp << ":" << ops_.chunkserverPort
                       << " Fail to register to MDS "
                       << mdsEps_[inServiceIndex_]
                       << ", cntl errorCode: " << cntl.ErrorCode() << ","
                       << " cntl error: " << cntl.ErrorText() << ","
                       << " statusCode: " << resp.statuscode() << ","
                       << " going to sleep and try again.";
            if (cntl.ErrorCode() == EHOSTDOWN ||
                cntl.ErrorCode() == brpc::ELOGOFF) {
                inServiceIndex_ = (inServiceIndex_ + 1) % mdsEps_.size();
            }
            sleep(1);
            --retries;
        }
    }

    if (retries <= 0) {
        LOG(ERROR) << ops_.chunkserverIp << ":" << ops_.chunkserverPort
                   << " Fail to register to MDS for " << ops_.registerRetries
                   << " times.";
        return -1;
    }

    metadata->set_version(CURRENT_METADATA_VERSION);
    metadata->set_id(resp.chunkserverid());
    metadata->set_token(resp.token());
    metadata->set_checksum(ChunkServerMetaHelper::MetadataCrc(*metadata));

    LOG(INFO) << ops_.chunkserverIp << ":" << ops_.chunkserverPort
              << " Successfully registered to MDS: " << mdsEps_[inServiceIndex_]
              << ", chunkserver id: " << metadata->id() << ","
              << " token: " << metadata->token() << ","
              << " persisting them to local storage.";

    if (PersistChunkServerMeta(*metadata) < 0) {
        LOG(ERROR) << "Failed to persist chunkserver meta data";
        return -1;
    }

    return 0;
}

int Register::PersistChunkServerMeta(const ChunkServerMetadata &metadata) {
    int fd;
    std::string metaFile =
        UriParser::GetPathFromUri(ops_.chunkserverMetaUri);

    std::string metaStr;
    if (!ChunkServerMetaHelper::EncodeChunkServerMeta(metadata, &metaStr)) {
        LOG(ERROR) << "Failed to encode chunkserver meta data.";
        return -1;
    }

    fd = ops_.fs->Open(metaFile.c_str(), O_RDWR | O_CREAT);
    if (fd < 0) {
        LOG(ERROR) << "Fail to open chunkserver metadata file for write";
        return -1;
    }

    if (ops_.fs->Write(
        fd, metaStr.c_str(), 0, metaStr.size()) < metaStr.size()) {
        LOG(ERROR) << "Failed to write chunkserver metadata file";
        return -1;
    }
    if (ops_.fs->Close(fd)) {
        LOG(ERROR) << "Failed to close chunkserver metadata file";
        return -1;
    }

    return 0;
}
}  // namespace chunkserver
}  // namespace curve
