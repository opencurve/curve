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

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/client/fuse_common.h"
#include "curvefs/src/client/extent_manager.h"
#include "curvefs/src/client/client_operator.h"
#include "src/common/timeutility.h"
#include "src/common/dummyserver.h"
#include "src/client/client_common.h"

#define PORT_LIMIT 65535

using ::curvefs::common::S3Info;
using ::curvefs::common::Volume;
using ::curvefs::mds::topology::PartitionTxId;
using ::curvefs::mds::FSStatusCode_Name;

#define RETURN_IF_UNSUCCESS(action)                                            \
    do {                                                                       \
        rc = renameOp.action();                                                \
        if (rc != CURVEFS_ERROR::OK) {                                         \
            return rc;                                                         \
        }                                                                      \
    } while (0)

namespace curvefs {
namespace client {

using common::MetaCacheOpt;
using rpcclient::ChannelManager;
using rpcclient::Cli2ClientImpl;
using rpcclient::MetaCache;

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
        metaClient_->Init(option.excutorOpt, metaCache, channelManager);
    if (ret2 != MetaStatusCode::OK) {
        return CURVEFS_ERROR::INTERNAL;
    }

    CURVEFS_ERROR ret3 =
        inodeManager_->Init(option.iCacheLruSize, option.enableICacheMetrics);
    if (ret3 != CURVEFS_ERROR::OK) {
        return ret3;
    }
    ret3 =
        dentryManager_->Init(option.dCacheLruSize, option.enableDCacheMetrics);
    return ret3;
}

void FuseClient::UnInit() {
    delete mdsBase_;
    mdsBase_ = nullptr;
}

CURVEFS_ERROR FuseClient::Run() {
    if (isStop_.exchange(false)) {
        flushThread_ = Thread(&FuseClient::FlushInodeLoop, this);
        LOG(INFO) << "Start fuse client flush thread ok.";
        return CURVEFS_ERROR::OK;
    }
    return CURVEFS_ERROR::INTERNAL;
}

void FuseClient::Fini() {
    if (!isStop_.exchange(true)) {
        LOG(INFO) << "stop fuse client flush thread ...";
        sleeper_.interrupt();
        flushThread_.join();
    }
    LOG(INFO) << "stop fuse client flush thread ok.";
}

void FuseClient::FlushInodeLoop() {
    while (sleeper_.wait_for(std::chrono::seconds(option_.flushPeriodSec))) {
        FlushInode();
    }
}

CURVEFS_ERROR FuseClient::FuseOpInit(void *userdata,
                                     struct fuse_conn_info *conn) {
    struct MountOption *mOpts = (struct MountOption *)userdata;
    std::string mountPointStr =
        (mOpts->mountPoint == nullptr) ? "" : mOpts->mountPoint;
    std::string fsName = (mOpts->fsName == nullptr) ? "" : mOpts->fsName;

    std::string mountPointWithHost;
    int retVal = AddHostNameToMountPointStr(mountPointStr, &mountPointWithHost);
    if (retVal < 0) {
        LOG(ERROR) << "AddHostNameToMountPointStr failed, ret = " << retVal;
        return CURVEFS_ERROR::INTERNAL;
    }

    FsInfo fsInfo;
    FSStatusCode ret = mdsClient_->GetFsInfo(fsName, &fsInfo);
    if (ret != FSStatusCode::OK) {
        if (FSStatusCode::NOT_FOUND == ret) {
            LOG(INFO) << "The fsName not exist, try to CreateFs"
                      << ", fsName = " << fsName;

            CURVEFS_ERROR ret2 = CreateFs(userdata, &fsInfo);
            if (ret2 != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "CreateFs failed, ret = " << ret2
                           << ", fsName = " << fsName;
                return ret2;
            }
        } else {
            LOG(ERROR) << "GetFsInfo failed, FSStatusCode = " << ret
                       << ", FSStatusCode_Name = "
                       << FSStatusCode_Name(ret)
                       << ", fsName = " << fsName;
            return CURVEFS_ERROR::INTERNAL;
        }
    }
    auto find = std::find(fsInfo.mountpoints().begin(),
                          fsInfo.mountpoints().end(), mountPointWithHost);
    if (find != fsInfo.mountpoints().end()) {
        LOG(ERROR) << "MountFs found mountPoint exist";
        return CURVEFS_ERROR::MOUNT_POINT_EXIST;
    }
    ret = mdsClient_->MountFs(fsName, mountPointWithHost, &fsInfo);
    if (ret != FSStatusCode::OK && ret != FSStatusCode::MOUNT_POINT_EXIST) {
        LOG(ERROR) << "MountFs failed, FSStatusCode = " << ret
                   << ", FSStatusCode_Name = "
                   << FSStatusCode_Name(ret)
                   << ", fsName = " << fsName
                   << ", mountPoint = " << mountPointWithHost;
        return CURVEFS_ERROR::MOUNT_FAILED;
    }
    fsInfo_ = std::make_shared<FsInfo>(fsInfo);
    inodeManager_->SetFsId(fsInfo.fsid());
    dentryManager_->SetFsId(fsInfo.fsid());
    LOG(INFO) << "Mount " << fsName << " on " << mountPointWithHost
              << " success!";

    fsMetric_ = std::make_shared<FSMetric>(fsName);

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
    std::string mountPointStr =
        (mOpts->mountPoint == nullptr) ? "" : mOpts->mountPoint;

    std::string mountPointWithHost;
    int retVal = AddHostNameToMountPointStr(mountPointStr, &mountPointWithHost);
    if (retVal < 0) {
        return;
    }
    FSStatusCode ret = mdsClient_->UmountFs(fsName, mountPointWithHost);
    if (ret != FSStatusCode::OK && ret != FSStatusCode::MOUNT_POINT_NOT_EXIST) {
        LOG(ERROR) << "UmountFs failed, FSStatusCode = " << ret
                   << ", FSStatusCode_Name = "
                   << FSStatusCode_Name(ret)
                   << ", fsName = " << fsName
                   << ", mountPoint = " << mountPointWithHost;
        return;
    }

    LOG(INFO) << "Umount " << fsName << " on " << mountPointWithHost
              << " success!";
    return;
}

std::ostream &operator<<(std::ostream &os, const struct stat &attr) {
    os << "{ st_ino = " << attr.st_ino << ", st_mode = " << attr.st_mode
       << ", st_nlink = " << attr.st_nlink << ", st_uid = " << attr.st_uid
       << ", st_gid = " << attr.st_gid << ", st_size = " << attr.st_size
       << ", st_atime = " << attr.st_atime << ", st_mtime = " << attr.st_mtime
       << ", st_ctime = " << attr.st_ctime << "}" << std::endl;
    return os;
}

void FuseClient::GetAttrFromInode(const Inode &inode, struct stat *attr) {
    attr->st_ino = inode.inodeid();
    attr->st_mode = inode.mode();
    attr->st_nlink = inode.nlink();
    attr->st_uid = inode.uid();
    attr->st_gid = inode.gid();
    attr->st_size = inode.length();
    attr->st_atime = inode.atime();
    attr->st_mtime = inode.mtime();
    attr->st_ctime = inode.ctime();
    VLOG(6) << "GetAttrFromInode attr =  " << *attr;
}

void FuseClient::GetDentryParamFromInode(const Inode &inode,
                                         fuse_entry_param *param) {
    memset(param, 0, sizeof(fuse_entry_param));
    param->ino = inode.inodeid();
    param->generation = 0;
    GetAttrFromInode(inode, &param->attr);
    param->attr_timeout = option_.attrTimeOut;
    param->entry_timeout = option_.entryTimeOut;
}

CURVEFS_ERROR FuseClient::FuseOpLookup(fuse_req_t req, fuse_ino_t parent,
                                       const char *name, fuse_entry_param *e) {
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
    std::shared_ptr<InodeWrapper> inodeWrapper;
    fuse_ino_t ino = dentry.inodeid();
    ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }
    Inode inode = inodeWrapper->GetInodeLocked();
    GetDentryParamFromInode(inode, e);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpOpen(fuse_req_t req, fuse_ino_t ino,
                                     struct fuse_file_info *fi) {
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();

    ret = inodeWrapper->Open();
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }

    uint64_t nowTime = TimeUtility::GetTimeofDaySec();
    if (fi->flags & O_TRUNC) {
        if (fi->flags & O_WRONLY || fi->flags & O_RDWR) {
            Inode inode = inodeWrapper->GetInodeUnlocked();
            CURVEFS_ERROR tRet = Truncate(&inode, 0);
            if (tRet != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "truncate file fail, ret = " << ret
                           << ", inodeid = " << ino;
                return CURVEFS_ERROR::INTERNAL;
            }
            inode.set_length(0);
            inode.set_ctime(nowTime);
            inode.set_mtime(nowTime);
            inodeWrapper->SwapInode(&inode);
            ret = inodeWrapper->Sync();
            if (ret != CURVEFS_ERROR::OK) {
                return ret;
            }
        } else {
            return CURVEFS_ERROR::NOPERMISSION;
        }
    }

    return ret;
}

CURVEFS_ERROR FuseClient::MakeNode(fuse_req_t req, fuse_ino_t parent,
                                   const char *name, mode_t mode,
                                   FsFileType type, fuse_entry_param *e) {
    if (strlen(name) > option_.maxNameLength) {
        return CURVEFS_ERROR::NAMETOOLONG;
    }
    const struct fuse_ctx *ctx = fuse_req_ctx(req);
    InodeParam param;
    param.fsId = fsInfo_->fsid();
    param.length = 0;
    param.uid = ctx->uid;
    param.gid = ctx->gid;
    param.mode = mode;
    param.type = type;

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

    VLOG(6) << "dentryManager_ CreateDentry success"
            << ", parent = " << parent << ", name = " << name
            << ", mode = " << mode;

    Inode inode = inodeWrapper->GetInodeLocked();
    GetDentryParamFromInode(inode, e);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpMkDir(fuse_req_t req, fuse_ino_t parent,
                                      const char *name, mode_t mode,
                                      fuse_entry_param *e) {
    // TODO(xuchaojie): mkdir should update parent inode nlink num
    return MakeNode(req, parent, name, S_IFDIR | mode,
                    FsFileType::TYPE_DIRECTORY, e);
}

CURVEFS_ERROR FuseClient::FuseOpUnlink(fuse_req_t req, fuse_ino_t parent,
                                       const char *name) {
    return RemoveNode(req, parent, name, false);
}

CURVEFS_ERROR FuseClient::FuseOpRmDir(fuse_req_t req, fuse_ino_t parent,
                                      const char *name) {
    return RemoveNode(req, parent, name, true);
}

CURVEFS_ERROR FuseClient::RemoveNode(fuse_req_t req, fuse_ino_t parent,
                                     const char *name, bool isDir) {
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

    if (isDir) {
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

    ret = dentryManager_->DeleteDentry(parent, name);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ DeleteDentry fail, ret = " << ret
                   << ", parent = " << parent << ", name = " << name;
        return ret;
    }
    std::shared_ptr<InodeWrapper> inodeWrapper;
    ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    ret = inodeWrapper->UnLinkLocked();
    // also return ok even if unlink failed.
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "UnLink failed, ret = " << ret << ", inodeid = " << ino
                   << ", parent = " << parent << ", name = " << name;
        return CURVEFS_ERROR::OK;
    }
    inodeManager_->ClearInodeCache(ino);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseClient::FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info *fi) {
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

    VLOG(6) << "FuseOpOpenDir, ino = " << ino << ", dindex = " << dindex;
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
                                           struct fuse_file_info *fi) {
    uint64_t dindex = fi->fh;
    dirBuf_->DirBufferRelease(dindex);
    VLOG(6) << "FuseOpReleaseDir, ino = " << ino << ", dindex = " << dindex;
    return CURVEFS_ERROR::OK;
}

static void dirbuf_add(fuse_req_t req, struct DirBufferHead *b,
                       const Dentry &dentry) {
    struct stat stbuf;
    size_t oldsize = b->size;
    b->size += fuse_add_direntry(req, NULL, 0, dentry.name().c_str(), NULL, 0);
    b->p = static_cast<char *>(realloc(b->p, b->size));
    memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = dentry.inodeid();
    fuse_add_direntry(req, b->p + oldsize, b->size - oldsize,
                      dentry.name().c_str(), &stbuf, b->size);
}

CURVEFS_ERROR FuseClient::FuseOpReadDir(fuse_req_t req, fuse_ino_t ino,
                                        size_t size, off_t off,
                                        struct fuse_file_info *fi,
                                        char **buffer, size_t *rSize) {
    VLOG(6) << "FuseOpReadDir ino = " << ino << ", size = " << size
            << ", off = " << off;
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();

    uint64_t dindex = fi->fh;
    DirBufferHead *bufHead = dirBuf_->DirBufferGet(dindex);
    if (!bufHead->wasRead) {
        std::list<Dentry> dentryList;
        auto limit = option_.listDentryLimit;
        ret = dentryManager_->ListDentry(ino, &dentryList, limit);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "dentryManager_ ListDentry fail, ret = " << ret
                       << ", parent = " << ino;
            return ret;
        }
        for (const auto &dentry : dentryList) {
            dirbuf_add(req, bufHead, dentry);
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
    VLOG(3) << "FuseOpRename from (" << parent << ", " << name << ") to ("
            << newparent << ", " << newname << ")";
    if (strlen(name) > option_.maxNameLength ||
        strlen(newname) > option_.maxNameLength) {
        return CURVEFS_ERROR::NAMETOOLONG;
    }
    auto renameOp =
        RenameOperator(fsInfo_->fsid(), parent, name, newparent, newname,
                       dentryManager_, inodeManager_, metaClient_, mdsClient_);

    CURVEFS_ERROR rc = CURVEFS_ERROR::OK;
    RETURN_IF_UNSUCCESS(GetTxId);
    RETURN_IF_UNSUCCESS(Precheck);
    RETURN_IF_UNSUCCESS(PrepareTx);
    RETURN_IF_UNSUCCESS(CommitTx);
    renameOp.UnlinkOldInode();
    renameOp.UpdateCache();
    return rc;
}

CURVEFS_ERROR FuseClient::FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info *fi,
                                        struct stat *attr) {
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }
    memset(attr, 0, sizeof(*attr));
    Inode inode = inodeWrapper->GetInodeLocked();
    GetAttrFromInode(inode, attr);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpSetAttr(fuse_req_t req, fuse_ino_t ino,
                                        struct stat *attr, int to_set,
                                        struct fuse_file_info *fi,
                                        struct stat *attrOut) {
    VLOG(3) << "FuseOpSetAttr to_set = " << to_set;
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    Inode inode = inodeWrapper->GetInodeUnlocked();

    uint64_t nowTime = TimeUtility::GetTimeofDaySec();
    if (to_set & FUSE_SET_ATTR_MODE) {
        inode.set_mode(attr->st_mode);
    }
    if (to_set & FUSE_SET_ATTR_UID) {
        inode.set_uid(attr->st_uid);
    }
    if (to_set & FUSE_SET_ATTR_GID) {
        inode.set_gid(attr->st_gid);
    }
    if (to_set & FUSE_SET_ATTR_SIZE) {
        CURVEFS_ERROR tRet = Truncate(&inode, attr->st_size);
        if (tRet != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "truncate file fail, ret = " << ret
                       << ", inodeid = " << ino;
            return tRet;
        }
        inode.set_length(attr->st_size);
    }
    if (to_set & FUSE_SET_ATTR_ATIME) {
        inode.set_atime(attr->st_atime);
    }
    if (to_set & FUSE_SET_ATTR_MTIME) {
        inode.set_mtime(attr->st_mtime);
    }
    if (to_set & FUSE_SET_ATTR_ATIME_NOW) {
        inode.set_atime(nowTime);
    }
    if (to_set & FUSE_SET_ATTR_MTIME_NOW) {
        inode.set_mtime(nowTime);
    }
    if (to_set & FUSE_SET_ATTR_CTIME) {
        inode.set_ctime(attr->st_ctime);
    } else {
        inode.set_ctime(nowTime);
    }
    inodeWrapper->UpdateInode(inode);
    ret = inodeWrapper->Sync();
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }
    memset(attrOut, 0, sizeof(*attrOut));
    GetAttrFromInode(inode, attrOut);
    return ret;
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

    Inode inode = inodeWrapper->GetInodeLocked();
    GetDentryParamFromInode(inode, e);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpLink(fuse_req_t req, fuse_ino_t ino,
                                     fuse_ino_t newparent, const char *newname,
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
    ret = inodeWrapper->LinkLocked();
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
    ret = dentryManager_->CreateDentry(dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ CreateDentry fail, ret = " << ret
                   << ", parent = " << newparent << ", name = " << newname;

        CURVEFS_ERROR ret2 = inodeWrapper->UnLinkLocked();
        if (ret2 != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "Also unlink inode failed, ret = " << ret2
                       << ", inodeid = " << inodeWrapper->GetInodeId();
        }
        return ret;
    }

    Inode inode = inodeWrapper->GetInodeLocked();
    GetDentryParamFromInode(inode, e);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpReadLink(fuse_req_t req, fuse_ino_t ino,
                                         std::string *linkStr) {
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }
    Inode inode = inodeWrapper->GetInodeLocked();
    *linkStr = inode.symlink();
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseClient::FuseOpRelease(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info *fi) {
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();

    ret = inodeWrapper->Release();
    return ret;
}

void FuseClient::FlushInode() { inodeManager_->FlushInodeOnce(); }

void FuseClient::FlushInodeAll() { inodeManager_->FlushAll(); }

void FuseClient::FlushAll() {
    FlushData();
    FlushInodeAll();
}

}  // namespace client
}  // namespace curvefs
