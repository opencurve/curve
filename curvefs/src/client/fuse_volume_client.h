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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef CURVEFS_SRC_CLIENT_FUSE_VOLUME_CLIENT_H_
#define CURVEFS_SRC_CLIENT_FUSE_VOLUME_CLIENT_H_

#include <memory>

#include "curvefs/src/client/fuse_client.h"

namespace curvefs {
namespace client {

using common::VolumeOption;

class FuseVolumeClient : public FuseClient {
 public:
    FuseVolumeClient()
      : FuseClient(),
    blockDeviceClient_(std::make_shared<BlockDeviceClientImpl>()) {}

    FuseVolumeClient(const std::shared_ptr<MdsClient> &mdsClient,
        const std::shared_ptr<MetaServerClient> &metaClient,
        const std::shared_ptr<SpaceClient> &spaceClient,
        const std::shared_ptr<InodeCacheManager> &inodeManager,
        const std::shared_ptr<DentryCacheManager> &dentryManager,
        const std::shared_ptr<ExtentManager> &extManager,
        const std::shared_ptr<BlockDeviceClient> &blockDeviceClient)
    : FuseClient(mdsClient, metaClient, spaceClient,
        inodeManager, dentryManager, extManager),
      blockDeviceClient_(blockDeviceClient) {}

    CURVEFS_ERROR Init(const FuseClientOption &option) override;

    void UnInit() override;

    void FuseOpInit(void *userdata, struct fuse_conn_info *conn) override;

    void FuseOpDestroy(void *userdata) override;

    CURVEFS_ERROR FuseOpWrite(fuse_req_t req, fuse_ino_t ino,
        const char *buf, size_t size, off_t off,
        struct fuse_file_info *fi, size_t *wSize) override;

    CURVEFS_ERROR FuseOpRead(fuse_req_t req,
            fuse_ino_t ino, size_t size, off_t off,
            struct fuse_file_info *fi,
            char *buffer,
            size_t *rSize) override;

    CURVEFS_ERROR FuseOpCreate(fuse_req_t req, fuse_ino_t parent,
        const char *name, mode_t mode, struct fuse_file_info *fi,
        fuse_entry_param *e) override;

    CURVEFS_ERROR FuseOpMkNod(fuse_req_t req, fuse_ino_t parent,
        const char *name, mode_t mode, dev_t rdev,
        fuse_entry_param *e) override;

 private:
    CURVEFS_ERROR Truncate(Inode *inode, uint64_t length) override;

 private:
    // curve client
    std::shared_ptr<BlockDeviceClient> blockDeviceClient_;

    VolumeOption volOpts_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FUSE_VOLUME_CLIENT_H_
