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

#include "curvefs/src/space_allocator/metaserver_client.h"

#include <glog/logging.h>

#include <algorithm>

namespace curvefs {
namespace space {

DEFINE_string(metaServerAddr, "0.0.0.0:20000", "metaserver service address");

bool MetaServerClient::Init(const MetaServerClientOption& opt) {
    opt_ = opt;

    if (opt_.addr.empty()) {
        opt_.addr = FLAGS_metaServerAddr;
    }

    int ret = channel_.Init(opt_.addr.c_str(), nullptr);
    if (ret != 0) {
        LOG(ERROR) << "Init channel to metaserver failed";
        return false;
    }

    return true;
}

bool MetaServerClient::GetAllInodeExtents(uint32_t fsId, uint64_t rootInodeId,
                                          Extents* exts) {
    return RecursiveListDentry(fsId, rootInodeId, exts, nullptr);
}

bool MetaServerClient::GetMaxChunkIdOfAllInode(uint32_t fsId,
                                               uint64_t rootInodeId,
                                               uint64_t* maxChunkId) {
    return RecursiveListDentry(fsId, rootInodeId, nullptr, maxChunkId);
}

// TODO(wuhanqing): Using recursion, if the file system has a deep layer, will
// blow up the stack?
bool MetaServerClient::RecursiveListDentry(uint32_t fsId, uint64_t inodeId,
                                           Extents* exts,
                                           uint64_t* maxChunkId) {
    if (exts == nullptr && maxChunkId == nullptr) {
        return false;
    } else if (exts != nullptr && maxChunkId != nullptr) {
        return false;
    }

    metaserver::MetaServerService_Stub stub(&channel_);
    metaserver::ListDentryRequest request;
    metaserver::ListDentryResponse response;

    brpc::Controller cntl;

    request.set_fsid(fsId);
    request.set_dirinodeid(inodeId);
    stub.ListDentry(&cntl, &request, &response, nullptr);

    if (cntl.Failed() || response.statuscode() != metaserver::OK) {
        return false;
    }

    for (auto& d : response.dentrys()) {
        metaserver::MetaServerService_Stub stub(&channel_);
        metaserver::GetInodeRequest request;
        metaserver::GetInodeResponse response;

        brpc::Controller cntl;
        request.set_fsid(fsId);
        request.set_inodeid(d.inodeid());

        stub.GetInode(&cntl, &request, &response, nullptr);

        if (cntl.Failed() || response.statuscode() != metaserver::OK) {
            return false;
        }

        switch (response.inode().type()) {
            case metaserver::FsFileType::TYPE_FILE:
                AppendExtents(exts, response.inode().volumeextentlist());
                break;
            case metaserver::FsFileType::TYPE_S3:
                SelectMaxChunkId(maxChunkId,
                                 response.inode().s3chunkinfolist());
                break;
            case metaserver::FsFileType::TYPE_DIRECTORY:
                return RecursiveListDentry(fsId, response.inode().inodeid(),
                                           exts, maxChunkId);
            default:
                break;
        }
    }

    return true;
}

void MetaServerClient::AppendExtents(
    Extents* exts,
    const curvefs::metaserver::VolumeExtentList& protoExts) const {
    const auto& volumeextents = protoExts.volumeextents();
    for (auto& e : volumeextents) {
        exts->emplace_back(e.volumeoffset(), e.length());
    }
}

void MetaServerClient::SelectMaxChunkId(
    uint64_t* chunkId,
    const curvefs::metaserver::S3ChunkInfoList& chunkinfos) const {
    const auto& chunks = chunkinfos.s3chunks();
    for (auto& e : chunks) {
        *chunkId = std::max(*chunkId, e.chunkid());
    }
}

}  // namespace space
}  // namespace curvefs
