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

#ifndef CURVEFS_SRC_CLIENT_FUSE_CLIENT_H_
#define CURVEFS_SRC_CLIENT_FUSE_CLIENT_H_

#include <memory>

#include "curvefs/src/client/fuse_common.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/dentry_cache_manager.h"
#include "curvefs/src/client/metaserver_client.h"
#include "curvefs/src/client/block_device_client.h"
#include "curvefs/src/client/mds_client.h"
#include "curvefs/src/client/dir_buffer.h"

namespace curvefs {
namespace client {

class FuseClient {
 public:
    FuseClient() {}

    int Init() { return 0; }
    void UnInit() {}

    void init(void *userdata, struct fuse_conn_info *conn);

    void destroy(void *userdata) {}

    void lookup(fuse_req_t req, fuse_ino_t parent, const char *name);

    void write(fuse_req_t req, fuse_ino_t ino, const char *buf,
              size_t size, off_t off, struct fuse_file_info *fi);

    void read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
              struct fuse_file_info *fi);

    void open(fuse_req_t req, fuse_ino_t ino,
              struct fuse_file_info *fi);

    void create(fuse_req_t req, fuse_ino_t parent, const char *name,
              mode_t mode, struct fuse_file_info *fi);

    void mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
              mode_t mode, dev_t rdev);

    void mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
               mode_t mode);

    void unlink(fuse_req_t req, fuse_ino_t parent, const char *name);

    void rmdir(fuse_req_t req, fuse_ino_t parent, const char *name);

    void opendir(fuse_req_t req, fuse_ino_t ino,
             struct fuse_file_info *fi);

    void readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
             struct fuse_file_info *fi);

    void getattr(fuse_req_t req, fuse_ino_t ino,
             struct fuse_file_info *fi);

    void setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
            int to_set, struct fuse_file_info *fi);

 private:
    void fuse_reply_err_by_errcode(fuse_req_t req, CURVEFS_ERROR errcode);

    void GetDentryParamFromInode(const Inode &inode, fuse_entry_param *param);

    void GetAttrFromInode(const Inode &inode, struct stat *attr);

 private:
    // filesystem info
    std::shared_ptr<FsInfo> fsInfo_;

    // inode cache manager
    std::shared_ptr<InodeCacheManager> inodeManager_;

    // dentry cache manager
    std::shared_ptr<DentryCacheManager> dentryManager_;

    // dir buffer
    std::shared_ptr<DirBuffer> dirBuf_;

    // metaserver client
    std::shared_ptr<MetaServerClient> metaClient_;

    // mds client
    std::shared_ptr<MdsClient> mdsClient_;

    // curve client
    std::shared_ptr<BlockDeviceClient> blockDeviceClient_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FUSE_CLIENT_H_
