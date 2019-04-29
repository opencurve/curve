/*
 * Project: curve
 * Created Date: Thur May 9th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <json2pb/pb_to_json.h>
#include <json2pb/json_to_pb.h>
#include <brpc/channel.h>
#include <fcntl.h>

#include <string>
#include <vector>

#include "src/common/crc32.h"
#include "src/chunkserver/register.h"
#include "src/chunkserver/chunkserver_helper.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"
#include "proto/topology.pb.h"

namespace curve {
namespace chunkserver {
int Register::RegisterToMDS(ChunkServerMetadata *metadata) {
    butil::ip_t mdsIp;
    if (butil::str2ip(ops_.mdsIp.c_str(), &mdsIp) < 0) {
        LOG(ERROR) << "Invalid MDS IP provided: " << ops_.mdsIp;
        return -1;
    }

    butil::EndPoint mdsEp = butil::EndPoint(mdsIp, ops_.mdsPort);
    LOG(INFO) << "MDS address is  " << ops_.mdsIp << ":" << ops_.mdsPort;

    // 元数据文件不存在，向MDS注册, 并把所有的内容持久化
    ::curve::mds::topology::ChunkServerRegistRequest req;
    ::curve::mds::topology::ChunkServerRegistResponse resp;

    req.set_disktype(ops_.chunkserverDiskType);
    req.set_diskpath(ops_.chunserverStoreUri);
    req.set_hostip(ops_.chunkserverIp);
    req.set_port(ops_.chunkserverPort);

    LOG(INFO) << "Registering to MDS " << mdsEp;
    int retries = ops_.registerRetries;
    while (retries >= 0) {
        brpc::Channel channel;
        brpc::Controller cntl;

        cntl.set_timeout_ms(ops_.registerTimeout);

        if (channel.Init(mdsEp, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to MDS " << mdsEp;
        }
        curve::mds::topology::TopologyService_Stub stub(&channel);

        stub.RegistChunkServer(&cntl, &req, &resp, nullptr);
        // TODO(lixiaocui): 后续错误码和mds共享后改成枚举类型
        if (!cntl.Failed() && resp.statuscode() == 0) {
            break;
        } else {
            LOG(ERROR) << "Fail to register to MDS " << mdsEp << ","
                       << " cntl error: " << cntl.ErrorText() << ","
                       << " statusCode: " << resp.statuscode() << ","
                       << " going to sleep and try again.";
            sleep(1);
            --retries;
        }
    }

    if (retries <= 0) {
        LOG(ERROR) << "Fail to register to MDS " << mdsEp
                   << " for " << ops_.registerRetries
                   << " times.";
        return -1;
    }

    metadata->set_version(CURRENT_METADATA_VERSION);
    metadata->set_id(resp.chunkserverid());
    metadata->set_token(resp.token());
    metadata->set_checksum(ChunkServerMetaHelper::MetadataCrc(*metadata));

    LOG(INFO) << "Successfully registered to MDS,"
              << " chunkserver id: " << metadata->id() << ","
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
        FsAdaptorUtil::GetPathFromUri(ops_.chunkserverMetaUri);

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
