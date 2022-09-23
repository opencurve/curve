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

#include "curvefs/src/client/fuse_client.h"

#include <list>
#include <algorithm>
#include <cstring>
#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <utility>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/fuse_common.h"
#include "curvefs/src/client/client_operator.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/src/client/xattr_manager.h"
#include "src/common/net_common.h"
#include "src/common/dummyserver.h"
#include "src/client/client_common.h"

#define PORT_LIMIT 65535

using ::curvefs::common::S3Info;
using ::curvefs::common::Volume;
using ::curvefs::mds::topology::PartitionTxId;
using ::curvefs::mds::FSStatusCode_Name;
using ::curvefs::client::common::MAXXATTRLENGTH;
using ::curvefs::client::common::FileHandle;

#define RETURN_IF_UNSUCCESS(action)                                            \
    do {                                                                       \
        rc = renameOp.action();                                                \
        if (rc != CURVEFS_ERROR::OK) {                                         \
            return rc;                                                         \
        }                                                                      \
    } while (0)


namespace curvefs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
}  // namespace common
}  // namespace client
}  // namespace curvefs

namespace curvefs {
namespace client {

using common::MetaCacheOpt;
using rpcclient::ChannelManager;
using rpcclient::Cli2ClientImpl;
using rpcclient::MetaCache;
using common::FLAGS_enableCto;

CURVEFS_ERROR FuseClient::Init(const FuseClientOption &option) {
    option_ = option;

    mdsBase_ = new MDSBaseClient();
    FSStatusCode ret = mdsClient_->Init(option.mdsOpt, mdsBase_);
    if (ret != FSStatusCode::OK) {
        return CURVEFS_ERROR::INTERNAL;
    }

    auto cli2Client = std::make_shared<Cli2ClientImpl>();
    auto metaCache = std::make_shared<MetaCache>();
    metaCache->Init(option.metaCacheOpt, cli2Client, mdsClient_);
    auto channelManager = std::make_shared<ChannelManager<MetaserverID>>();

    leaseExecutor_ =
        std::make_shared<LeaseExecutor>(option.leaseOpt, metaCache, mdsClient_);

    xattrManager_ = std::make_shared<XattrManager>(inodeManager_,
        dentryManager_, option_.listDentryLimit, option_.listDentryThreads);

    uint32_t listenPort = 0;
    if (!curve::common::StartBrpcDummyserver(option.dummyServerStartPort,
                                             PORT_LIMIT, &listenPort)) {
        return CURVEFS_ERROR::INTERNAL;
    }

    std::string localIp;
    if (!curve::common::NetCommon::GetLocalIP(&localIp)) {
        LOG(ERROR) << "Get local ip failed!";
        return CURVEFS_ERROR::INTERNAL;
    }
    curve::client::ClientDummyServerInfo::GetInstance().SetPort(listenPort);
    curve::client::ClientDummyServerInfo::GetInstance().SetIP(localIp);

    MetaStatusCode ret2 =
        metaClient_->Init(option.excutorOpt, option.excutorInternalOpt,
                          metaCache, channelManager);
    if (ret2 != MetaStatusCode::OK) {
        return CURVEFS_ERROR::INTERNAL;
    }

    CURVEFS_ERROR ret3 =
        inodeManager_->Init(option.iCacheLruSize, option.enableICacheMetrics,
                            option.flushPeriodSec);
    if (ret3 != CURVEFS_ERROR::OK) {
        return ret3;
    }

    ret3 =
        dentryManager_->Init(option.dCacheLruSize, option.enableDCacheMetrics);
    if (ret3 != CURVEFS_ERROR::OK) {
        return ret3;
    }

    return ret3;
}

void FuseClient::UnInit() {
    delete mdsBase_;
    mdsBase_ = nullptr;
}

CURVEFS_ERROR FuseClient::Run() {
    if (isStop_.exchange(false)) {
        inodeManager_->Run();
        return CURVEFS_ERROR::OK;
    }
    return CURVEFS_ERROR::INTERNAL;
}

void FuseClient::Fini() {
    if (!isStop_.exchange(true)) {
        inodeManager_->Stop();
        xattrManager_->Stop();
    }
}

CURVEFS_ERROR FuseClient::FuseOpInit(void *userdata,
                                     struct fuse_conn_info *conn) {
    struct MountOption* mOpts = (struct MountOption*)userdata;
    // set path
    mountpoint_.set_path((mOpts->mountPoint == nullptr) ? ""
                                                       : mOpts->mountPoint);
    std::string fsName = (mOpts->fsName == nullptr) ? "" : mOpts->fsName;

    mountpoint_.set_cto(FLAGS_enableCto);

    int retVal = SetHostPortInMountPoint(&mountpoint_);
    if (retVal < 0) {
        LOG(ERROR) << "Set Host and Port in MountPoint failed, ret = "
                   << retVal;
        return CURVEFS_ERROR::INTERNAL;
    }

    auto ret = mdsClient_->MountFs(fsName, mountpoint_, fsInfo_.get());
    if (ret != FSStatusCode::OK && ret != FSStatusCode::MOUNT_POINT_EXIST) {
        LOG(ERROR) << "MountFs failed, FSStatusCode = " << ret
                   << ", FSStatusCode_Name = "
                   << FSStatusCode_Name(ret)
                   << ", fsName = " << fsName
                   << ", mountPoint = " << mountpoint_.ShortDebugString();
        return CURVEFS_ERROR::MOUNT_FAILED;
    }

    inodeManager_->SetFsId(fsInfo_->fsid());
    dentryManager_->SetFsId(fsInfo_->fsid());
    enableSumInDir_ = fsInfo_->enablesumindir() && !FLAGS_enableCto;
    LOG(INFO) << "Mount " << fsName << " on " << mountpoint_.ShortDebugString()
              << " success!" << " enableSumInDir = " << enableSumInDir_;

    fsMetric_ = std::make_shared<FSMetric>(fsName);

    // init fsname and mountpoint
    leaseExecutor_->SetFsName(fsName);
    leaseExecutor_->SetMountPoint(mountpoint_);
    if (!leaseExecutor_->Start()) {
        return CURVEFS_ERROR::INTERNAL;
    }

    init_ = true;

    return CURVEFS_ERROR::OK;
}

void FuseClient::FuseOpDestroy(void *userdata) {
    if (!init_) {
        return;
    }

    FlushAll();
    dirBuf_->DirBufferFreeAll();

    struct MountOption *mOpts = (struct MountOption *)userdata;
    std::string fsName = (mOpts->fsName == nullptr) ? "" : mOpts->fsName;

    Mountpoint mountPoint;
    mountPoint.set_path((mOpts->mountPoint == nullptr) ? ""
                                                       : mOpts->mountPoint);
    int retVal = SetHostPortInMountPoint(&mountPoint);
    if (retVal < 0) {
        return;
    }
    LOG(INFO) << "Umount " << fsName << " on " << mountPoint.ShortDebugString()
              << " start";

    FSStatusCode ret = mdsClient_->UmountFs(fsName, mountPoint);
    if (ret != FSStatusCode::OK && ret != FSStatusCode::MOUNT_POINT_NOT_EXIST) {
        LOG(ERROR) << "UmountFs failed, FSStatusCode = " << ret
                   << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                   << ", fsName = " << fsName
                   << ", mountPoint = " << mountPoint.ShortDebugString();
        return;
    }

    LOG(INFO) << "Umount " << fsName << " on " << mountPoint.ShortDebugString()
              << " success!";
    return;
}

void InodeAttr2ParamAttr(const InodeAttr &inodeAttr, struct stat *attr) {
    attr->st_ino = inodeAttr.inodeid();
    attr->st_mode = inodeAttr.mode();
    attr->st_nlink = inodeAttr.nlink();
    attr->st_uid = inodeAttr.uid();
    attr->st_gid = inodeAttr.gid();
    attr->st_size = inodeAttr.length();
    attr->st_rdev = inodeAttr.rdev();
    attr->st_atim.tv_sec = inodeAttr.atime();
    attr->st_atim.tv_nsec = inodeAttr.atime_ns();
    attr->st_mtim.tv_sec = inodeAttr.mtime();
    attr->st_mtim.tv_nsec = inodeAttr.mtime_ns();
    attr->st_ctim.tv_sec = inodeAttr.ctime();
    attr->st_ctim.tv_nsec = inodeAttr.ctime_ns();
    attr->st_blksize = kOptimalIOBlockSize;

    switch (inodeAttr.type()) {
        case metaserver::TYPE_S3:
            attr->st_blocks = (inodeAttr.length() + 511) / 512;
            break;
        default:
            attr->st_blocks = 0;
            break;
    }
}

void GetDentryParamFromInodeAttr(
    const FuseClientOption &option,
    const InodeAttr &inodeAttr,
    fuse_entry_param *param) {
    memset(param, 0, sizeof(fuse_entry_param));
    param->ino = inodeAttr.inodeid();
    param->generation = 0;
    InodeAttr2ParamAttr(inodeAttr, &param->attr);
    param->attr_timeout = option.attrTimeOut;
    param->entry_timeout = option.entryTimeOut;
}

CURVEFS_ERROR FuseClient::FuseOpLookup(fuse_req_t req, fuse_ino_t parent,
                                       const char *name, fuse_entry_param *e) {
    VLOG(1) << "FuseOpLookup parent: " << parent
            << ", name: " << name;
    if (strlen(name) > option_.maxNameLength) {
        return CURVEFS_ERROR::NAMETOOLONG;
    }

    Dentry dentry;
    CURVEFS_ERROR ret = dentryManager_->GetDentry(parent, name, &dentry);
    if (ret != CURVEFS_ERROR::OK) {
        if (ret != CURVEFS_ERROR::NOTEXIST) {
            LOG(WARNING) << "dentryManager_ get dentry fail, ret = " << ret
                         << ", parent inodeid = " << parent
                         << ", name = " << name;
        }
        return ret;
    }

    fuse_ino_t ino = dentry.inodeid();
    InodeAttr attr;
    ret = inodeManager_->GetInodeAttr(ino, &attr);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inodeAttr fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    GetDentryParamFromInodeAttr(option_, attr, e);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpOpen(fuse_req_t req, fuse_ino_t ino,
                                     struct fuse_file_info *fi) {
    VLOG(1) << "FuseOpOpen, ino: " << ino;
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    if (fi->flags & O_TRUNC) {
        if (fi->flags & O_WRONLY || fi->flags & O_RDWR) {
            uint64_t length = inodeWrapper->GetLengthLocked();
            CURVEFS_ERROR tRet = Truncate(inodeWrapper.get(), 0);
            if (tRet != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "truncate file fail, ret = " << ret
                           << ", inodeid = " << ino;
                return CURVEFS_ERROR::INTERNAL;
            }
            inodeWrapper->SetLengthLocked(0);
            inodeWrapper->UpdateTimestampLocked(kChangeTime | kModifyTime);
            if (length != 0) {
                ret = inodeWrapper->Sync();
                if (ret != CURVEFS_ERROR::OK) {
                    return ret;
                }
            } else {
                inodeWrapper->MarkDirty();
            }

            if (enableSumInDir_ && length != 0) {
                // update parent summary info
                const Inode *inode = inodeWrapper->GetInodeLocked();
                XAttr xattr;
                xattr.mutable_xattrinfos()->insert({XATTRFBYTES,
                    std::to_string(length)});
                for (const auto &it : inode->parent()) {
                    auto tret = xattrManager_->UpdateParentInodeXattr(
                        it, xattr, false);
                    if (tret != CURVEFS_ERROR::OK) {
                        LOG(ERROR) << "UpdateParentInodeXattr failed,"
                                   << " inodeId = " << it
                                   << ", xattr = " << xattr.DebugString();
                    }
                }
            }
        } else {
            return CURVEFS_ERROR::NOPERMISSION;
        }
    }
    if (FLAGS_enableCto) {
        inodeManager_->AddOpenedInode(ino);
    }
    return ret;
}

CURVEFS_ERROR FuseClient::UpdateParentInodeMCTimeAndInvalidNlink(
    fuse_ino_t parent, FsFileType type) {
    std::shared_ptr<InodeWrapper> parentInodeWrapper;
    auto ret = inodeManager_->GetInode(parent, parentInodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << parent;
        return ret;
    }

    {
        curve::common::UniqueLock lk = parentInodeWrapper->GetUniqueLock();
        parentInodeWrapper->UpdateTimestampLocked(kModifyTime | kChangeTime);

        if (FsFileType::TYPE_DIRECTORY == type) {
            parentInodeWrapper->InvalidateNlink();
        }
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseClient::MakeNode(fuse_req_t req, fuse_ino_t parent,
                                   const char *name, mode_t mode,
                                   FsFileType type, dev_t rdev,
                                   fuse_entry_param *e) {
    if (strlen(name) > option_.maxNameLength) {
        return CURVEFS_ERROR::NAMETOOLONG;
    }
    const struct fuse_ctx *ctx = fuse_req_ctx(req);
    InodeParam param;
    param.fsId = fsInfo_->fsid();
    if (FsFileType::TYPE_DIRECTORY == type) {
        param.length = 4096;
    } else {
        param.length = 0;
    }
    param.uid = ctx->uid;
    param.gid = ctx->gid;
    param.mode = mode;
    param.type = type;
    param.rdev = rdev;
    param.parent = parent;

    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->CreateInode(param, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager CreateInode fail, ret = " << ret
                   << ", parent = " << parent << ", name = " << name
                   << ", mode = " << mode;
        return ret;
    }

    VLOG(6) << "inodeManager CreateInode success"
            << ", parent = " << parent << ", name = " << name
            << ", mode = " << mode
            << ", inode id = " << inodeWrapper->GetInodeId();

    Dentry dentry;
    dentry.set_fsid(fsInfo_->fsid());
    dentry.set_inodeid(inodeWrapper->GetInodeId());
    dentry.set_parentinodeid(parent);
    dentry.set_name(name);
    dentry.set_type(inodeWrapper->GetType());
    if (type == FsFileType::TYPE_FILE || type == FsFileType::TYPE_S3) {
        dentry.set_flag(DentryFlag::TYPE_FILE_FLAG);
    }

    ret = dentryManager_->CreateDentry(dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ CreateDentry fail, ret = " << ret
                   << ", parent = " << parent << ", name = " << name
                   << ", mode = " << mode;

        CURVEFS_ERROR ret2 =
            inodeManager_->DeleteInode(inodeWrapper->GetInodeId());
        if (ret2 != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "Also delete inode failed, ret = " << ret2
                       << ", inodeid = " << inodeWrapper->GetInodeId();
        }
        return ret;
    }

    ret = UpdateParentInodeMCTimeAndInvalidNlink(parent, type);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "UpdateParentInodeMCTimeAndInvalidNlink failed"
                   << ", parent: " << parent
                   << ", name: " << name
                   << ", type: " << type;
        return ret;
    }

    VLOG(6) << "dentryManager_ CreateDentry success"
            << ", parent = " << parent << ", name = " << name
            << ", mode = " << mode;

    if (enableSumInDir_) {
        // update parent summary info
        XAttr xattr;
        xattr.mutable_xattrinfos()->insert({XATTRENTRIES, "1"});
        if (type == FsFileType::TYPE_DIRECTORY) {
            xattr.mutable_xattrinfos()->insert({XATTRSUBDIRS, "1"});
        } else {
            xattr.mutable_xattrinfos()->insert({XATTRFILES, "1"});
        }
        xattr.mutable_xattrinfos()->insert({XATTRFBYTES,
            std::to_string(inodeWrapper->GetLength())});
        auto tret = xattrManager_->UpdateParentInodeXattr(parent, xattr, true);
        if (tret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "UpdateParentInodeXattr failed,"
                       << " inodeId = " << parent
                       << ", xattr = " << xattr.DebugString();
        }
    }

    InodeAttr attr;
    inodeWrapper->GetInodeAttr(&attr);
    GetDentryParamFromInodeAttr(option_, attr, e);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpMkDir(fuse_req_t req, fuse_ino_t parent,
                                      const char *name, mode_t mode,
                                      fuse_entry_param *e) {
    VLOG(1) << "FuseOpMkDir, parent: " << parent << ", name: " << name
            << ", mode: " << mode;
    return MakeNode(req, parent, name, S_IFDIR | mode,
                    FsFileType::TYPE_DIRECTORY, 0, e);
}

CURVEFS_ERROR FuseClient::FuseOpRmDir(fuse_req_t req, fuse_ino_t parent,
                                      const char *name) {
    VLOG(1) << "FuseOpRmDir, parent: " << parent << ", name: " << name;
    return RemoveNode(req, parent, name, FsFileType::TYPE_DIRECTORY);
}

CURVEFS_ERROR FuseClient::RemoveNode(fuse_req_t req, fuse_ino_t parent,
                                     const char *name, FsFileType type) {
    if (strlen(name) > option_.maxNameLength) {
        return CURVEFS_ERROR::NAMETOOLONG;
    }
    Dentry dentry;
    CURVEFS_ERROR ret = dentryManager_->GetDentry(parent, name, &dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(WARNING) << "dentryManager_ GetDentry fail, ret = " << ret
                     << ", parent = " << parent << ", name = " << name;
        return ret;
    }

    uint64_t ino = dentry.inodeid();

    if (FsFileType::TYPE_DIRECTORY == type) {
        std::list<Dentry> dentryList;
        auto limit = option_.listDentryLimit;
        ret = dentryManager_->ListDentry(ino, &dentryList, limit);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "dentryManager_ ListDentry fail, ret = " << ret
                       << ", parent = " << ino;
            return ret;
        }
        if (!dentryList.empty()) {
            LOG(ERROR) << "rmdir not empty";
            return CURVEFS_ERROR::NOTEMPTY;
        }
    }

    ret = dentryManager_->DeleteDentry(parent, name, type);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ DeleteDentry fail, ret = " << ret
                   << ", parent = " << parent << ", name = " << name;
        return ret;
    }

    ret = UpdateParentInodeMCTimeAndInvalidNlink(parent, type);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "UpdateParentInodeMCTimeAndInvalidNlink failed"
                   << ", parent: " << parent
                   << ", name: " << name
                   << ", type: " << type;
        return ret;
    }

    std::shared_ptr<InodeWrapper> inodeWrapper;
    ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    ret = inodeWrapper->UnLink(parent);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "UnLink failed, ret = " << ret << ", inodeid = " << ino
                   << ", parent = " << parent << ", name = " << name;
    }

    if (enableSumInDir_) {
        // update parent summary info
        XAttr xattr;
        xattr.mutable_xattrinfos()->insert({XATTRENTRIES, "1"});
        if (FsFileType::TYPE_DIRECTORY == type) {
            xattr.mutable_xattrinfos()->insert({XATTRSUBDIRS, "1"});
        } else {
            xattr.mutable_xattrinfos()->insert({XATTRFILES, "1"});
        }
        xattr.mutable_xattrinfos()->insert({XATTRFBYTES,
            std::to_string(inodeWrapper->GetLength())});
        auto tret = xattrManager_->UpdateParentInodeXattr(parent, xattr, false);
        if (tret != CURVEFS_ERROR::OK) {
            LOG(WARNING) << "UpdateParentInodeXattr failed,"
                         << " inodeId = " << parent
                         << ", xattr = " << xattr.DebugString();
        }
    }

    inodeManager_->ClearInodeCache(ino);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info *fi) {
    VLOG(1) << "FuseOpOpenDir ino = " << ino;
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();

    uint64_t dindex = dirBuf_->DirBufferNew();
    fi->fh = dindex;
    VLOG(1) << "FuseOpOpenDir, ino: " << ino << ", dindex: " << dindex;
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
                                           struct fuse_file_info *fi) {
    uint64_t dindex = fi->fh;
    VLOG(1) << "FuseOpReleaseDir, ino: " << ino << ", dindex: " << dindex;
    dirBuf_->DirBufferRelease(dindex);

    // release inodeAttr cache
    inodeManager_->ReleaseCache(ino);
    return CURVEFS_ERROR::OK;
}

static void dirbuf_add(fuse_req_t req, struct DirBufferHead *b,
                       const Dentry &dentry,
                       const FuseClientOption &option,
                       bool cacheDir,
                       InodeAttr *attr = nullptr) {
    struct stat stbuf;
    struct fuse_entry_param param;
    size_t oldsize = b->size;
    if (!cacheDir) {
        b->size += fuse_add_direntry(req, NULL, 0, dentry.name().c_str(),
                                     NULL, 0);
        b->p = static_cast<char *>(realloc(b->p, b->size));
        memset(&stbuf, 0, sizeof(stbuf));
        stbuf.st_ino = dentry.inodeid();
        fuse_add_direntry(req, b->p + oldsize, b->size - oldsize,
                          dentry.name().c_str(), &stbuf, b->size);
    } else {
        b->size += fuse_add_direntry_plus(req, NULL, 0, dentry.name().c_str(),
                                          NULL, 0);
        b->p = static_cast<char *>(realloc(b->p, b->size));
        GetDentryParamFromInodeAttr(option, *attr, &param);
        fuse_add_direntry_plus(req, b->p + oldsize, b->size - oldsize,
                               dentry.name().c_str(), &param, b->size);
    }
}

CURVEFS_ERROR FuseClient::FuseOpReadDirPlus(fuse_req_t req, fuse_ino_t ino,
                                            size_t size, off_t off,
                                            struct fuse_file_info *fi,
                                            char **buffer, size_t *rSize,
                                            bool cacheDir) {
    VLOG(1) << "FuseOpReadDirPlus ino: " << ino << ", size: " << size
            << ", off = " << off << ", cacheDir = " << cacheDir;
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    uint64_t dindex = fi->fh;
    DirBufferHead *bufHead = dirBuf_->DirBufferGet(dindex);
    if (!bufHead->wasRead) {
        std::list<Dentry> dentryList;
        std::set<uint64_t> inodeIds;
        std::map<uint64_t, InodeAttr> inodeAttrMap;
        auto limit = option_.listDentryLimit;
        ret = dentryManager_->ListDentry(ino, &dentryList, limit);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "dentryManager_ ListDentry fail, ret = " << ret
                       << ", parent = " << ino;
            return ret;
        }

        if (!cacheDir) {
            for (const auto &dentry : dentryList) {
                dirbuf_add(req, bufHead, dentry, option_, cacheDir);
            }
        } else {
            for (const auto &dentry : dentryList) {
                inodeIds.emplace(dentry.inodeid());
            }
            VLOG(3) << "batch get inode size = " << inodeIds.size();
            ret = inodeManager_->BatchGetInodeAttrAsync(ino, &inodeIds,
                                                        &inodeAttrMap);
            if (ret != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "BatchGetInodeAttr failed when FuseOpReadDir"
                           << ", parentId = " << ino;
                return ret;
            }

            for (const auto &dentry : dentryList) {
                auto iter = inodeAttrMap.find(dentry.inodeid());
                if (iter != inodeAttrMap.end()) {
                    dirbuf_add(req, bufHead, dentry, option_,
                               cacheDir, &iter->second);
                } else {
                    LOG(WARNING) << "BatchGetInodeAttr missing some inodes,"
                                 << " inodeId = " << dentry.inodeid();
                }
            }
        }
        bufHead->wasRead = true;
    }

    if (off < bufHead->size) {
        *buffer = bufHead->p + off;
        *rSize = std::min(bufHead->size - off, size);
    } else {
        *buffer = nullptr;
        *rSize = 0;
    }
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpRename(fuse_req_t req, fuse_ino_t parent,
                                       const char *name, fuse_ino_t newparent,
                                       const char *newname) {
    VLOG(1) << "FuseOpRename from (" << parent << ", " << name << ") to ("
            << newparent << ", " << newname << ")";
    if (strlen(name) > option_.maxNameLength ||
        strlen(newname) > option_.maxNameLength) {
        return CURVEFS_ERROR::NAMETOOLONG;
    }

    auto renameOp =
        RenameOperator(fsInfo_->fsid(), fsInfo_->fsname(),
                       parent, name, newparent, newname,
                       dentryManager_, inodeManager_, metaClient_, mdsClient_,
                       option_.enableMultiMountPointRename);

    curve::common::LockGuard lg(renameMutex_);
    CURVEFS_ERROR rc = CURVEFS_ERROR::OK;
    VLOG(3) << "FuseOpRename [start]: " << renameOp.DebugString();
    RETURN_IF_UNSUCCESS(GetTxId);
    RETURN_IF_UNSUCCESS(Precheck);
    RETURN_IF_UNSUCCESS(RecordOldInodeInfo);
    // Do not move LinkDestParentInode behind CommitTx.
    // If so, the nlink will be lost when the machine goes down
    RETURN_IF_UNSUCCESS(LinkDestParentInode);
    RETURN_IF_UNSUCCESS(PrepareTx);
    RETURN_IF_UNSUCCESS(CommitTx);
    VLOG(3) << "FuseOpRename [success]: " << renameOp.DebugString();
    // Do not check UnlinkSrcParentInode, beause rename is already success
    renameOp.UnlinkSrcParentInode();
    renameOp.UnlinkOldInode();
    renameOp.UpdateInodeParent();
    renameOp.UpdateCache();

    if (enableSumInDir_) {
        xattrManager_->UpdateParentXattrAfterRename(
            parent, newparent, newname, &renameOp);
    }

    return rc;
}

CURVEFS_ERROR FuseClient::FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info *fi,
                                        struct stat *attr) {
    VLOG(1) << "FuseOpGetAttr ino = " << ino;
    InodeAttr inodeAttr;
    CURVEFS_ERROR ret =
        inodeManager_->GetInodeAttr(ino, &inodeAttr);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inodeAttr fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }
    InodeAttr2ParamAttr(inodeAttr, attr);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpSetAttr(fuse_req_t req, fuse_ino_t ino,
                                        struct stat *attr, int to_set,
                                        struct fuse_file_info *fi,
                                        struct stat *attrOut) {
    VLOG(1) << "FuseOpSetAttr to_set: " << to_set << ", ino: " << ino
            << ", attr: " << *attr;
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    if (to_set & FUSE_SET_ATTR_MODE) {
        inodeWrapper->SetMode(attr->st_mode);
    }
    if (to_set & FUSE_SET_ATTR_UID) {
        inodeWrapper->SetUid(attr->st_uid);
    }
    if (to_set & FUSE_SET_ATTR_GID) {
        inodeWrapper->SetGid(attr->st_gid);
    }

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);

    if (to_set & FUSE_SET_ATTR_ATIME) {
        inodeWrapper->UpdateTimestampLocked(attr->st_atim, kAccessTime);
    }
    if (to_set & FUSE_SET_ATTR_ATIME_NOW) {
        inodeWrapper->UpdateTimestampLocked(now, kAccessTime);
    }
    if (to_set & FUSE_SET_ATTR_MTIME) {
        inodeWrapper->UpdateTimestampLocked(attr->st_mtim, kModifyTime);
    }
    if (to_set & FUSE_SET_ATTR_MTIME_NOW) {
        inodeWrapper->UpdateTimestampLocked(now, kModifyTime);
    }
    if (to_set & FUSE_SET_ATTR_CTIME) {
        inodeWrapper->UpdateTimestampLocked(attr->st_ctim, kChangeTime);
    } else {
        inodeWrapper->UpdateTimestampLocked(now, kChangeTime);
    }

    if (to_set & FUSE_SET_ATTR_SIZE) {
        int64_t changeSize =
            attr->st_size -
            static_cast<int64_t>(inodeWrapper->GetLengthLocked());
        CURVEFS_ERROR tRet = Truncate(inodeWrapper.get(), attr->st_size);
        if (tRet != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "truncate file fail, ret = " << ret
                       << ", inodeid = " << ino;
            return tRet;
        }
        inodeWrapper->SetLengthLocked(attr->st_size);
        ret = inodeWrapper->Sync();
        if (ret != CURVEFS_ERROR::OK) {
            return ret;
        }
        InodeAttr inodeAttr;
        inodeWrapper->GetInodeAttrLocked(&inodeAttr);
        InodeAttr2ParamAttr(inodeAttr, attrOut);

        if (enableSumInDir_ && changeSize != 0) {
            // update parent summary info
            const Inode* inode = inodeWrapper->GetInodeLocked();
            XAttr xattr;
            xattr.mutable_xattrinfos()->insert({XATTRFBYTES,
                std::to_string(std::abs(changeSize))});
            bool direction = changeSize > 0;
            for (const auto &it : inode->parent()) {
                auto tret = xattrManager_->UpdateParentInodeXattr(
                    it, xattr, direction);
                if (tret != CURVEFS_ERROR::OK) {
                    LOG(ERROR) << "UpdateParentInodeXattr failed,"
                               << " inodeId = " << it
                               << ", xattr = " << xattr.DebugString();
                }
            }
        }
        return ret;
    }
    ret = inodeWrapper->SyncAttr();
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }
    InodeAttr inodeAttr;
    inodeWrapper->GetInodeAttrLocked(&inodeAttr);
    InodeAttr2ParamAttr(inodeAttr, attrOut);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpGetXattr(fuse_req_t req, fuse_ino_t ino,
                                         const char* name, void* value,
                                         size_t size) {
    VLOG(9) << "FuseOpGetXattr, ino: " << ino
            << ", name: " << name << ", size = " << size;
    if (option_.disableXattr) {
        return CURVEFS_ERROR::NOTSUPPORT;
    }

    std::string xValue;
    InodeAttr inodeAttr;
    CURVEFS_ERROR ret = inodeManager_->GetInodeAttr(ino, &inodeAttr);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inodeAttr fail, ret = " << ret
                    << ", inodeid = " << ino;
        return ret;
    }

    ret = xattrManager_->GetXattr(name, &xValue, &inodeAttr, enableSumInDir_);
    if (CURVEFS_ERROR::OK != ret) {
        LOG(ERROR) << "xattrManager get xattr failed, name = " << name;
        return ret;
    }

    ret = CURVEFS_ERROR::NODATA;
    if (xValue.length() > 0) {
        if ((size == 0 && xValue.length() <= MAXXATTRLENGTH) ||
            (size >= xValue.length() && xValue.length() <= MAXXATTRLENGTH)) {
            memcpy(value, xValue.c_str(), xValue.length());
            ret = CURVEFS_ERROR::OK;
        } else {
            ret = CURVEFS_ERROR::OUT_OF_RANGE;
        }
    }
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpSetXattr(fuse_req_t req, fuse_ino_t ino,
                                         const char* name, const char* value,
                                         size_t size, int flags) {
    VLOG(1) << "FuseOpSetXattr ino: " << ino << ", name: " << name
            << ", value: " << value;
    if (option_.disableXattr) {
        return CURVEFS_ERROR::NOTSUPPORT;
    }

    std::string strname(name);
    std::string strvalue(value, size);
    if (strname.length() > MAXXATTRLENGTH  || size > MAXXATTRLENGTH) {
        LOG(ERROR) << "xattr length is too long, name = " << name
                   << ", name length = " << strname.length()
                   << ", value length = " << size;
        return CURVEFS_ERROR::OUT_OF_RANGE;
    }

    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    inodeWrapper->SetXattrLocked(strname, strvalue);
    ret = inodeWrapper->SyncAttr();
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "set xattr fail, ret = " << ret << ", inodeid = " << ino
                   << ", name = " << strname << ", value = " << strvalue;
        return ret;
    }
    VLOG(1) << "FuseOpSetXattr end";
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseClient::FuseOpListXattr(fuse_req_t req, fuse_ino_t ino,
                            char *value, size_t size, size_t *realSize) {
    VLOG(1) << "FuseOpListXattr, ino: " << ino << ", size = " << size;
    InodeAttr inodeAttr;
    CURVEFS_ERROR ret = inodeManager_->GetInodeAttr(ino, &inodeAttr);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inodeAttr fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    // get xattr key
    for (const auto &it : inodeAttr.xattr()) {
        // +1 because, the format is key\0key\0
        *realSize += it.first.length() + 1;
    }

    // add summary xattr key
    if (inodeAttr.type() == FsFileType::TYPE_DIRECTORY) {
        *realSize += strlen(XATTRRFILES) + 1;
        *realSize += strlen(XATTRRSUBDIRS) + 1;
        *realSize += strlen(XATTRRENTRIES) + 1;
        *realSize += strlen(XATTRRFBYTES) + 1;
    }

    if (size == 0) {
        return CURVEFS_ERROR::OK;
    } else if (size >= *realSize) {
        for (const auto &it : inodeAttr.xattr()) {
            auto tsize = it.first.length() + 1;
            memcpy(value, it.first.c_str(), tsize);
            value += tsize;
        }
        if (inodeAttr.type() == FsFileType::TYPE_DIRECTORY) {
            memcpy(value, XATTRRFILES, strlen(XATTRRFILES) + 1);
            value += strlen(XATTRRFILES) + 1;
            memcpy(value, XATTRRSUBDIRS, strlen(XATTRRSUBDIRS) + 1);
            value += strlen(XATTRRSUBDIRS) + 1;
            memcpy(value, XATTRRENTRIES, strlen(XATTRRENTRIES) + 1);
            value += strlen(XATTRRENTRIES) + 1;
            memcpy(value, XATTRRFBYTES, strlen(XATTRRFBYTES) + 1);
            value += strlen(XATTRRFBYTES) + 1;
        }
        return CURVEFS_ERROR::OK;
    }
    return CURVEFS_ERROR::OUT_OF_RANGE;
}

CURVEFS_ERROR FuseClient::FuseOpSymlink(fuse_req_t req, const char *link,
                                        fuse_ino_t parent, const char *name,
                                        fuse_entry_param *e) {
    if (strlen(name) > option_.maxNameLength) {
        return CURVEFS_ERROR::NAMETOOLONG;
    }
    const struct fuse_ctx *ctx = fuse_req_ctx(req);
    InodeParam param;
    param.fsId = fsInfo_->fsid();
    param.length = std::strlen(link);
    param.uid = ctx->uid;
    param.gid = ctx->gid;
    param.mode = S_IFLNK | 0777;
    param.type = FsFileType::TYPE_SYM_LINK;
    param.symlink = link;
    param.parent = parent;

    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->CreateInode(param, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager CreateInode fail, ret = " << ret
                   << ", parent = " << parent << ", name = " << name
                   << ", mode = " << param.mode;
        return ret;
    }
    Dentry dentry;
    dentry.set_fsid(fsInfo_->fsid());
    dentry.set_inodeid(inodeWrapper->GetInodeId());
    dentry.set_parentinodeid(parent);
    dentry.set_name(name);
    dentry.set_type(inodeWrapper->GetType());
    ret = dentryManager_->CreateDentry(dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ CreateDentry fail, ret = " << ret
                   << ", parent = " << parent << ", name = " << name
                   << ", mode = " << param.mode;

        CURVEFS_ERROR ret2 =
            inodeManager_->DeleteInode(inodeWrapper->GetInodeId());
        if (ret2 != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "Also delete inode failed, ret = " << ret2
                       << ", inodeid = " << inodeWrapper->GetInodeId();
        }
        return ret;
    }

    ret = UpdateParentInodeMCTimeAndInvalidNlink(
        parent, FsFileType::TYPE_SYM_LINK);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "UpdateParentInodeMCTimeAndInvalidNlink failed"
                   << ", link:" << link
                   << ", parent: " << parent
                   << ", name: " << name
                   << ", type: " << FsFileType::TYPE_SYM_LINK;
        return ret;
    }

    if (enableSumInDir_) {
        // update parent summary info
        XAttr xattr;
        xattr.mutable_xattrinfos()->insert({XATTRENTRIES, "1"});
        xattr.mutable_xattrinfos()->insert({XATTRFILES, "1"});
        xattr.mutable_xattrinfos()->insert({XATTRFBYTES,
            std::to_string(inodeWrapper->GetLength())});
        auto tret = xattrManager_->UpdateParentInodeXattr(parent, xattr, true);
        if (tret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "UpdateParentInodeXattr failed,"
                       << " inodeId = " << parent
                       << ", xattr = " << xattr.DebugString();
        }
    }

    InodeAttr attr;
    inodeWrapper->GetInodeAttr(&attr);
    GetDentryParamFromInodeAttr(option_, attr, e);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpLink(fuse_req_t req, fuse_ino_t ino,
                                     fuse_ino_t newparent, const char *newname,
                                     FsFileType type,
                                     fuse_entry_param *e) {
    if (strlen(newname) > option_.maxNameLength) {
        return CURVEFS_ERROR::NAMETOOLONG;
    }
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }
    ret = inodeWrapper->Link(newparent);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "Link Inode fail, ret = " << ret << ", inodeid = " << ino
                   << ", newparent = " << newparent
                   << ", newname = " << newname;
        return ret;
    }
    Dentry dentry;
    dentry.set_fsid(fsInfo_->fsid());
    dentry.set_inodeid(inodeWrapper->GetInodeId());
    dentry.set_parentinodeid(newparent);
    dentry.set_name(newname);
    dentry.set_type(inodeWrapper->GetType());
    ret = dentryManager_->CreateDentry(dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ CreateDentry fail, ret = " << ret
                   << ", parent = " << newparent << ", name = " << newname;

        CURVEFS_ERROR ret2 = inodeWrapper->UnLink(newparent);
        if (ret2 != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "Also unlink inode failed, ret = " << ret2
                       << ", inodeid = " << inodeWrapper->GetInodeId();
        }
        return ret;
    }

    ret = UpdateParentInodeMCTimeAndInvalidNlink(newparent, type);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "UpdateParentInodeMCTimeAndInvalidNlink failed"
                   << ", parent: " << newparent
                   << ", name: " << newname
                   << ", type: " << type;
        return ret;
    }

    if (enableSumInDir_) {
        // update parent summary info
        XAttr xattr;
        xattr.mutable_xattrinfos()->insert({XATTRENTRIES, "1"});
        xattr.mutable_xattrinfos()->insert({XATTRFILES, "1"});
        xattr.mutable_xattrinfos()->insert({XATTRFBYTES,
            std::to_string(inodeWrapper->GetLength())});
        auto tret = xattrManager_->UpdateParentInodeXattr(
            newparent, xattr, true);
        if (tret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "UpdateParentInodeXattr failed,"
                       << " inodeId = " << newparent
                       << ", xattr = " << xattr.DebugString();
        }
    }

    InodeAttr attr;
    inodeWrapper->GetInodeAttr(&attr);
    GetDentryParamFromInodeAttr(option_, attr, e);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpReadLink(fuse_req_t req, fuse_ino_t ino,
                                         std::string *linkStr) {
    VLOG(1) << "FuseOpReadLink, ino: " << ino << ", linkStr: " << linkStr;
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }
    *linkStr = inodeWrapper->GetSymlinkStr();
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseClient::FuseOpRelease(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info *fi) {
    VLOG(1) << "FuseOpRelease, ino: " << ino;
    if (FLAGS_enableCto) {
        inodeManager_->RemoveOpenedInode(ino);
    }
    return CURVEFS_ERROR::OK;
}

void FuseClient::FlushInode() { inodeManager_->FlushInodeOnce(); }

void FuseClient::FlushInodeAll() { inodeManager_->FlushAll(); }

void FuseClient::FlushAll() {
    FlushData();
    FlushInodeAll();
}

}  // namespace client
}  // namespace curvefs
