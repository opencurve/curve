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
#include "curvefs/src/client/volume/volume_storage.h"
#include "curvefs/src/volume/block_device_client.h"
#include "curvefs/src/volume/space_manager.h"

namespace curvefs {
namespace client {

using common::VolumeOption;
using ::curvefs::volume::BlockDeviceClient;
using ::curvefs::volume::BlockDeviceClientImpl;
using ::curvefs::volume::SpaceManager;

class FuseVolumeClient : public FuseClient {
 public:
    FuseVolumeClient()
        : FuseClient(),
          blockDeviceClient_(std::make_shared<BlockDeviceClientImpl>()) {}

    // for UNIT_TEST
    FuseVolumeClient(
        const std::shared_ptr<MdsClient> &mdsClient,
        const std::shared_ptr<MetaServerClient> &metaClient,
        const std::shared_ptr<InodeCacheManager> &inodeManager,
        const std::shared_ptr<DentryCacheManager> &dentryManager,
        const std::shared_ptr<BlockDeviceClient> &blockDeviceClient)
        : FuseClient(mdsClient, metaClient, inodeManager, dentryManager,
                     nullptr),
          blockDeviceClient_(blockDeviceClient) {}

    CURVEFS_ERROR Init(const FuseClientOption &option) override;

    void UnInit() override;

    CURVEFS_ERROR FuseOpInit(
        void *userdata, struct fuse_conn_info *conn) override;

    CURVEFS_ERROR FuseOpWrite(fuse_req_t req, fuse_ino_t ino,
        const char *buf, size_t size, off_t off,
        struct fuse_file_info *fi, FileOut* fileOut) override;

    CURVEFS_ERROR FuseOpRead(fuse_req_t req,
            fuse_ino_t ino, size_t size, off_t off,
            struct fuse_file_info *fi,
            char *buffer,
            size_t *rSize) override;

    CURVEFS_ERROR FuseOpCreate(fuse_req_t req,
                               fuse_ino_t parent,
                               const char* name,
                               mode_t mode,
                               struct fuse_file_info* fi,
                               EntryOut* entryOut) override;

    CURVEFS_ERROR FuseOpMkNod(fuse_req_t req,
                              fuse_ino_t parent,
                              const char* name,
                              mode_t mode,
                              dev_t rdev,
                              EntryOut* entryOut) override;

    CURVEFS_ERROR FuseOpLink(fuse_req_t req, fuse_ino_t ino,
                             fuse_ino_t newparent,
                             const char *newname,
                             EntryOut* entryOut) override;

    CURVEFS_ERROR FuseOpUnlink(fuse_req_t req, fuse_ino_t parent,
                               const char *name) override;

    CURVEFS_ERROR FuseOpFsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                              struct fuse_file_info *fi) override;

    CURVEFS_ERROR FuseOpFlush(fuse_req_t req, fuse_ino_t ino,
                              struct fuse_file_info *fi) override;

    CURVEFS_ERROR Truncate(InodeWrapper *inode, uint64_t length) override;

    void SetSpaceManagerForTesting(SpaceManager *manager);

    void SetVolumeStorageForTesting(VolumeStorage *storage);

 private:
    void FlushData() override;

 private:
    std::shared_ptr<BlockDeviceClient> blockDeviceClient_;
    std::unique_ptr<SpaceManager> spaceManager_;
    std::unique_ptr<VolumeStorage> storage_;

    VolumeOption volOpts_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FUSE_VOLUME_CLIENT_H_
