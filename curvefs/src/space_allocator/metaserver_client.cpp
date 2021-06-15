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

#include "curvefs/proto/metaserver.pb.h"

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
                                          std::vector<PExtent>* exts) {
    return RecursiveListDentry(fsId, rootInodeId, exts);
}

// TODO(wuhanqing): Using recursion, if the file system has a deep layer, will
// the stack burst?
bool MetaServerClient::RecursiveListDentry(uint32_t fsId, uint64_t inodeId,
                                           std::vector<PExtent>* exts) {
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

        if (response.inode().type() == metaserver::FsFileType::TYPE_FILE) {
            for (auto& e :
                 response.inode().volumeextentlist().volumeextents()) {
                exts->emplace_back(e.volumeoffset(), e.length());
            }
        } else if (response.inode().type() ==
                   metaserver::FsFileType::TYPE_DIRECTORY) {
            return RecursiveListDentry(fsId, response.inode().inodeid(), exts);
        }
    }

    return true;
}

}  // namespace space
}  // namespace curvefs
