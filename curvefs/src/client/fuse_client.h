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

#include <unistd.h>

#include <map>
#include <memory>
#include <string>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/client/block_device_client.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/dentry_cache_manager.h"
#include "curvefs/src/client/dir_buffer.h"
#include "curvefs/src/client/fuse_common.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/common/fast_align.h"
#include "src/common/concurrent/concurrent.h"

#define DirectIOAlignemnt 512

using ::curve::common::Atomic;
using ::curve::common::InterruptibleSleeper;
using ::curve::common::Thread;
using ::curvefs::common::FSType;
using ::curvefs::metaserver::DentryFlag;

namespace curvefs {
namespace client {

using common::FuseClientOption;
using rpcclient::MDSBaseClient;
using rpcclient::MdsClient;
using rpcclient::MdsClientImpl;
using rpcclient::MetaServerClient;
using rpcclient::MetaServerClientImpl;

using curvefs::common::is_aligned;

const uint32_t kMaxHostNameLength = 255u;

class FuseClient {
 public:
    FuseClient()
      : mdsClient_(std::make_shared<MdsClientImpl>()),
        metaClient_(std::make_shared<MetaServerClientImpl>()),
        inodeManager_(std::make_shared<InodeCacheManagerImpl>(metaClient_)),
        dentryManager_(std::make_shared<DentryCacheManagerImpl>(metaClient_)),
        dirBuf_(std::make_shared<DirBuffer>()),
        fsInfo_(nullptr),
        mdsBase_(nullptr),
        isStop_(true),
        init_(false) {}

    virtual ~FuseClient() {}

    FuseClient(const std::shared_ptr<MdsClient> &mdsClient,
        const std::shared_ptr<MetaServerClient> &metaClient,
        const std::shared_ptr<InodeCacheManager> &inodeManager,
        const std::shared_ptr<DentryCacheManager> &dentryManager)
          : mdsClient_(mdsClient),
            metaClient_(metaClient),
            inodeManager_(inodeManager),
            dentryManager_(dentryManager),
            dirBuf_(std::make_shared<DirBuffer>()),
            fsInfo_(nullptr),
            mdsBase_(nullptr),
            isStop_(true),
            init_(false) {}

    virtual CURVEFS_ERROR Init(const FuseClientOption &option);

    virtual void UnInit();

    virtual CURVEFS_ERROR Run();

    virtual void Fini();

    virtual void FuseOpInit(void* userdata, struct fuse_conn_info* conn);

    virtual void FuseOpDestroy(void* userdata);

    virtual CURVEFS_ERROR FuseOpWrite(fuse_req_t req, fuse_ino_t ino,
                                      const char* buf, size_t size, off_t off,
                                      struct fuse_file_info* fi,
                                      size_t* wSize) = 0;

    virtual CURVEFS_ERROR FuseOpRead(fuse_req_t req, fuse_ino_t ino,
                                     size_t size, off_t off,
                                     struct fuse_file_info* fi, char* buffer,
                                     size_t* rSize) = 0;

    virtual CURVEFS_ERROR FuseOpLookup(fuse_req_t req, fuse_ino_t parent,
                                       const char* name, fuse_entry_param* e);

    virtual CURVEFS_ERROR FuseOpOpen(fuse_req_t req, fuse_ino_t ino,
                                     struct fuse_file_info* fi);

    virtual CURVEFS_ERROR FuseOpCreate(fuse_req_t req, fuse_ino_t parent,
                                       const char* name, mode_t mode,
                                       struct fuse_file_info* fi,
                                       fuse_entry_param* e) = 0;

    virtual CURVEFS_ERROR FuseOpMkNod(fuse_req_t req, fuse_ino_t parent,
                                      const char* name, mode_t mode, dev_t rdev,
                                      fuse_entry_param* e) = 0;

    virtual CURVEFS_ERROR FuseOpMkDir(fuse_req_t req, fuse_ino_t parent,
                                      const char* name, mode_t mode,
                                      fuse_entry_param* e);

    virtual CURVEFS_ERROR FuseOpUnlink(fuse_req_t req, fuse_ino_t parent,
                                       const char* name);

    virtual CURVEFS_ERROR FuseOpRmDir(fuse_req_t req, fuse_ino_t parent,
                                      const char* name);

    virtual CURVEFS_ERROR FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info* fi);

    virtual CURVEFS_ERROR FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
                                           struct fuse_file_info* fi);

    virtual CURVEFS_ERROR FuseOpReadDir(fuse_req_t req, fuse_ino_t ino,
                                        size_t size, off_t off,
                                        struct fuse_file_info* fi,
                                        char** buffer, size_t* rSize);

    virtual CURVEFS_ERROR FuseOpRename(fuse_req_t req, fuse_ino_t parent,
                                       const char* name, fuse_ino_t newparent,
                                       const char* newname);

    virtual CURVEFS_ERROR FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info* fi,
                                        struct stat* attr);

    virtual CURVEFS_ERROR FuseOpSetAttr(fuse_req_t req, fuse_ino_t ino,
                                        struct stat* attr, int to_set,
                                        struct fuse_file_info* fi,
                                        struct stat* attrOut);

    virtual CURVEFS_ERROR FuseOpSymlink(fuse_req_t req, const char* link,
                                        fuse_ino_t parent, const char* name,
                                        fuse_entry_param* e);

    virtual CURVEFS_ERROR FuseOpLink(fuse_req_t req, fuse_ino_t ino,
                                     fuse_ino_t newparent, const char* newname,
                                     fuse_entry_param* e);

    virtual CURVEFS_ERROR FuseOpReadLink(fuse_req_t req, fuse_ino_t ino,
                                         std::string* linkStr);

    virtual CURVEFS_ERROR FuseOpRelease(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info* fi);

    virtual CURVEFS_ERROR FuseOpFsync(fuse_req_t req, fuse_ino_t ino,
                                      int datasync,
                                      struct fuse_file_info* fi) = 0;

    void SetFsInfo(std::shared_ptr<FsInfo> fsInfo) {
        fsInfo_ = fsInfo;
        init_ = true;
    }

    std::shared_ptr<FsInfo> GetFsInfo() {
        return fsInfo_;
    }

    virtual void FlushInode();

    virtual void FlushAll();

 protected:
    void GetDentryParamFromInode(const Inode& inode, fuse_entry_param* param);

    void GetAttrFromInode(const Inode& inode, struct stat* attr);

    CURVEFS_ERROR MakeNode(fuse_req_t req, fuse_ino_t parent, const char* name,
                           mode_t mode, FsFileType type, fuse_entry_param* e);

    CURVEFS_ERROR RemoveNode(fuse_req_t req, fuse_ino_t parent,
                             const char* name, bool idDir);

    int AddHostNameToMountPointStr(const std::string& mountPointStr,
                                   std::string* out) {
        char hostname[kMaxHostNameLength];
        int ret = gethostname(hostname, kMaxHostNameLength);
        if (ret < 0) {
            LOG(ERROR) << "GetHostName failed, ret = " << ret;
            return ret;
        }
        *out = std::string(hostname) + ":" + mountPointStr;
        return 0;
    }

 private:
    virtual CURVEFS_ERROR Truncate(Inode* inode, uint64_t length) = 0;

    virtual void FlushInodeLoop();

    virtual void FlushData() = 0;

    virtual CURVEFS_ERROR CreateFs(void* userdata, FsInfo* fsInfo) = 0;

 protected:
    // mds client
    std::shared_ptr<MdsClient> mdsClient_;

    // metaserver client
    std::shared_ptr<MetaServerClient> metaClient_;

    // inode cache manager
    std::shared_ptr<InodeCacheManager> inodeManager_;

    // dentry cache manager
    std::shared_ptr<DentryCacheManager> dentryManager_;

    // dir buffer
    std::shared_ptr<DirBuffer> dirBuf_;

    // filesystem info
    std::shared_ptr<FsInfo> fsInfo_;

    FuseClientOption option_;

    // init flags
    bool init_;

 private:
    MDSBaseClient* mdsBase_;

    Atomic<bool> isStop_;

    InterruptibleSleeper sleeper_;

    Thread flushThread_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FUSE_CLIENT_H_
