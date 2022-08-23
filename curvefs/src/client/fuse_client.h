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
#include <sys/stat.h>

#include <map>
#include <memory>
#include <string>
#include <list>
#include <utility>
#include <vector>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/dentry_cache_manager.h"
#include "curvefs/src/client/dir_buffer.h"
#include "curvefs/src/client/fuse_common.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/common/fast_align.h"
#include "curvefs/src/client/metric/client_metric.h"
#include "src/common/concurrent/concurrent.h"
#include "curvefs/src/common/define.h"
#include "curvefs/src/common/s3util.h"
#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/client_operator.h"
#include "curvefs/src/client/lease/lease_excutor.h"
#include "curvefs/src/client/xattr_manager.h"

#define DirectIOAlignment 512
#define WARMUP_CHECKINTERVAL_US 1000*1000
#define WARMUP_THREADS 10

using ::curve::common::Atomic;
using ::curve::common::InterruptibleSleeper;
using ::curve::common::Thread;
using ::curvefs::common::FSType;
using ::curvefs::metaserver::DentryFlag;
using ::curvefs::client::metric::FSMetric;

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

using mds::Mountpoint;
typedef struct WarmUpFileContext {
    fuse_ino_t inode;
    uint64_t fileLen;
    bool exist;
} WarmUpFileContext_t;
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
        init_(false),
        mounted_(false),
        enableSumInDir_(false) {}

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
            init_(false),
            mounted_(false),
            enableSumInDir_(false) {}

    virtual CURVEFS_ERROR Init(const FuseClientOption &option);

    virtual void UnInit();

    virtual CURVEFS_ERROR Run();

    virtual void Fini();

    virtual CURVEFS_ERROR FuseOpInit(
        void* userdata, struct fuse_conn_info* conn);

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
                                       const char* name) = 0;

    virtual CURVEFS_ERROR FuseOpRmDir(fuse_req_t req, fuse_ino_t parent,
                                      const char* name);

    virtual CURVEFS_ERROR FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info* fi);

    virtual CURVEFS_ERROR FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
                                           struct fuse_file_info* fi);

    virtual CURVEFS_ERROR FuseOpReadDirPlus(fuse_req_t req, fuse_ino_t ino,
                                            size_t size, off_t off,
                                            struct fuse_file_info* fi,
                                            char** buffer, size_t* rSize,
                                            bool cacheDir);

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

    virtual CURVEFS_ERROR FuseOpGetXattr(fuse_req_t req, fuse_ino_t ino,
                                         const char* name, void* value,
                                         size_t size);

    virtual CURVEFS_ERROR FuseOpListXattr(fuse_req_t req, fuse_ino_t ino,
                            char *value, size_t size, size_t *realSize);

    virtual CURVEFS_ERROR FuseOpSymlink(fuse_req_t req, const char* link,
                                        fuse_ino_t parent, const char* name,
                                        fuse_entry_param* e);

    virtual CURVEFS_ERROR FuseOpLink(fuse_req_t req, fuse_ino_t ino,
                                     fuse_ino_t newparent, const char* newname,
                                     fuse_entry_param* e) = 0;

    virtual CURVEFS_ERROR FuseOpReadLink(fuse_req_t req, fuse_ino_t ino,
                                         std::string* linkStr);

    virtual CURVEFS_ERROR FuseOpRelease(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info* fi);

    virtual CURVEFS_ERROR FuseOpFsync(fuse_req_t req, fuse_ino_t ino,
                                      int datasync,
                                      struct fuse_file_info* fi) = 0;
    virtual CURVEFS_ERROR FuseOpFlush(fuse_req_t req, fuse_ino_t ino,
                                      struct fuse_file_info *fi) {
        return CURVEFS_ERROR::OK;
    }

    virtual CURVEFS_ERROR FuseOpStatFs(fuse_req_t req, fuse_ino_t ino,
                                       struct statvfs* stbuf) {
        // TODO(chengyi01,wuhanqing): implement in s3 and volume client
        stbuf->f_frsize = stbuf->f_bsize = fsInfo_->blocksize();
        stbuf->f_blocks = 10UL << 30;
        stbuf->f_bavail = stbuf->f_bfree = stbuf->f_blocks - 1;

        stbuf->f_files = 1UL << 30;
        stbuf->f_ffree = stbuf->f_favail = stbuf->f_files - 1;

        stbuf->f_fsid = fsInfo_->fsid();

        stbuf->f_flag = 0;
        stbuf->f_namemax = option_.maxNameLength;
        return CURVEFS_ERROR::OK;
    }

    void SetFsInfo(const std::shared_ptr<FsInfo>& fsInfo) {
        fsInfo_ = fsInfo;
        init_ = true;
    }

    void SetMounted(bool mounted) {
        mounted_ = mounted;
    }

    TaskThreadPool<bthread::Mutex, bthread::ConditionVariable> &
      GetTaskFetchPool() {
        return taskFetchMetaPool_;
    }

    std::shared_ptr<FsInfo> GetFsInfo() {
        return fsInfo_;
    }

    virtual void FlushInode();

    virtual void FlushInodeAll();

    virtual void FlushAll();

    // for unit test
    void SetEnableSumInDir(bool enable) {
        enableSumInDir_ = enable;
    }
    std::list<fuse_ino_t>& GetReadAheadFiles() {
        std::unique_lock<std::mutex> lck(fetchMtx_);
        return readAheadFiles_;
    }

    void GetWarmUpFile(WarmUpFileContext_t* warmUpFile) {
        std::unique_lock<std::mutex> lck(warmUpFileMtx_);
        *warmUpFile = std::move(warmUpFile_);
        warmUpFile_.exist = false;
        return;
    }
    void SetWarmUpFile(WarmUpFileContext_t warmUpFile) {
        std::unique_lock<std::mutex> lck(warmUpFileMtx_);
        warmUpFile_ = warmUpFile;
        warmUpFile_.exist = true;
    }
    bool hasWarmUpTask() {
        std::unique_lock<std::mutex> lck(warmUpFileMtx_);
        return warmUpFile_.exist;
    }

    void FetchDentryEnqueue(std::string file);

    void PutWarmTask(const std::string& warmUpTask) {
        std::unique_lock<std::mutex> lck(warmUpTaskMtx_);
        warmUpTasks_.push_back(warmUpTask);
        WarmUpRun();
    }

 protected:
    CURVEFS_ERROR MakeNode(fuse_req_t req, fuse_ino_t parent, const char* name,
                           mode_t mode, FsFileType type, dev_t rdev,
                           fuse_entry_param* e);

    CURVEFS_ERROR RemoveNode(fuse_req_t req, fuse_ino_t parent,
                             const char* name, FsFileType type);

    CURVEFS_ERROR FuseOpLink(fuse_req_t req, fuse_ino_t ino,
                             fuse_ino_t newparent, const char* newname,
                             FsFileType type,
                             fuse_entry_param* e);

    int SetHostPortInMountPoint(Mountpoint* out) {
        char hostname[kMaxHostNameLength];
        int ret = gethostname(hostname, kMaxHostNameLength);
        if (ret < 0) {
            LOG(ERROR) << "GetHostName failed, ret = " << ret;
            return ret;
        }
        out->set_hostname(hostname);
        out->set_port(
            curve::client::ClientDummyServerInfo::GetInstance().GetPort());
        return 0;
    }

    void splitStr(const std::string& srcStr, const std::string& delimiter,
      std::vector<std::string>* splitPath) {
        char* pToken = nullptr;
        char* pSave = nullptr;
        pToken = strtok_r(const_cast<char*>(srcStr.c_str()),
          const_cast<char*>(delimiter.c_str()), &pSave);
        if (nullptr == pToken) {
            VLOG(6) << "del lookpath end";
            return;
        }
        splitPath->push_back(pToken);
        while (true) {
            pToken = strtok_r(NULL, const_cast<char*>(
              delimiter.c_str()), &pSave);
            if (nullptr == pToken) {
                VLOG(6) << "del lookpath end";
                break;
            }
            VLOG(9) << "del pToken is:" << pToken
                    << "pSave:" << pSave;
            splitPath->push_back(pToken);
        }
    }

 private:
    virtual CURVEFS_ERROR Truncate(InodeWrapper* inode, uint64_t length) = 0;

    virtual void FlushData() = 0;

    CURVEFS_ERROR UpdateParentInodeMCTimeAndInvalidNlink(
        fuse_ino_t parent, FsFileType type);

    void WarmUpTask();

    void WarmUpRun() {
        std::lock_guard<std::mutex> lk(mtx_);
        runned_ = true;
        cond_.notify_one();
    }
    void WaitWarmUp() {
        std::unique_lock<std::mutex> lk(mtx_);
        cond_.wait(lk, [this]() { return runned_; });
        runned_ = false;
    }
    std::mutex mtx_;
    std::condition_variable cond_;
    bool runned_ = false;


 protected:
    // mds client
    std::shared_ptr<MdsClient> mdsClient_;

    // metaserver client
    std::shared_ptr<MetaServerClient> metaClient_;

    // inode cache manager
    std::shared_ptr<InodeCacheManager> inodeManager_;

    // dentry cache manager
    std::shared_ptr<DentryCacheManager> dentryManager_;

    // xattr manager
    std::shared_ptr<XattrManager> xattrManager_;

    std::shared_ptr<LeaseExecutor> leaseExecutor_;

    // dir buffer
    std::shared_ptr<DirBuffer> dirBuf_;

    // filesystem info
    std::shared_ptr<FsInfo> fsInfo_;

    FuseClientOption option_;

    // init flags
    bool init_;

    std::atomic<bool>  mounted_;

    // enable record summary info in dir inode xattr
    bool enableSumInDir_;

    std::shared_ptr<FSMetric> fsMetric_;

    Mountpoint mountpoint_;

 private:
    MDSBaseClient* mdsBase_;

    Atomic<bool> isStop_;

    curve::common::Mutex renameMutex_;

    Thread bgCmdTaskThread_;
    std::atomic<bool> bgCmdStop_;
    std::mutex cmdMtx_;

    void FetchChildDentryEnqueue(fuse_ino_t  ino);
    void FetchChildDentry(fuse_ino_t ino);
    void FetchDentry(fuse_ino_t ino, std::string file);
    void LookPath(std::string file);
    TaskThreadPool<bthread::Mutex, bthread::ConditionVariable>
        taskFetchMetaPool_;

    // need warmup files
    std::list<fuse_ino_t> readAheadFiles_;
    std::mutex fetchMtx_;

    //  one warmup file provided by the user
    WarmUpFileContext_t warmUpFile_;
    std::mutex warmUpFileMtx_;

    std::list<std::string> warmUpTasks_;  // todo: need size control ?
    std::mutex warmUpTaskMtx_;

    void GetwarmTask(std::string *warmUpTask) {
        std::unique_lock<std::mutex> lck(warmUpTaskMtx_);
        if (warmUpTasks_.empty())
            return;
        *warmUpTask = std::move(warmUpTasks_.front());
        warmUpTasks_.pop_front();
    }
    bool hasWarmTask() {
        std::unique_lock<std::mutex> lck(warmUpTaskMtx_);
        return !warmUpTasks_.empty();
    }
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FUSE_CLIENT_H_
