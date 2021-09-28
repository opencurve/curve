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

#include <string>
#include <memory>

#include "curvefs/src/client/curve_fuse_op.h"
#include "curvefs/src/client/fuse_client.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/common/config.h"
#include "src/common/configuration.h"
#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/fuse_volume_client.h"
#include "curvefs/src/client/fuse_s3_client.h"

using ::curve::common::Configuration;
using ::curvefs::client::FuseClient;
using ::curvefs::client::common::FuseClientOption;
using ::curvefs::client::FuseVolumeClient;
using ::curvefs::client::FuseS3Client;
using ::curvefs::client::CURVEFS_ERROR;

static FuseClient *g_ClientInstance = nullptr;
static FuseClientOption *fuseClientOption = nullptr;

int InitGlog(const char *confPath, const char *argv0) {
    Configuration conf;
    conf.SetConfigPath(confPath);
    if (!conf.LoadConfig()) {
        LOG(ERROR) << "LoadConfig failed, confPath = " << confPath;
        return -1;
    }

    if (FLAGS_log_dir.empty()) {
        if (!conf.GetStringValue("client.common.logDir", &FLAGS_log_dir)) {
            LOG(WARNING) << "no client.common.logDir in " << confPath
                         << ", will log to /tmp";
        }
    }
    // initialize logging module
    google::InitGoogleLogging(argv0);
    return 0;
}

int InitFuseClient(const char *confPath, const char* fsType) {
    Configuration conf;
    conf.SetConfigPath(confPath);
    if (!conf.LoadConfig()) {
        LOG(ERROR) << "LoadConfig failed, confPath = " << confPath;
        return -1;
    }

    conf.PrintConfig();

    std::string fsTypeStr = (fsType == nullptr) ? "" : fsType;

    fuseClientOption = new FuseClientOption();
    curvefs::client::common::InitFuseClientOption(&conf, fuseClientOption);

    if (fsTypeStr == "s3") {
        g_ClientInstance =
            new FuseS3Client();
    } else if (fsTypeStr == "volume") {
        g_ClientInstance =
            new FuseVolumeClient();
    } else {
        LOG(ERROR) << "fsTypeStr invalid, which is " << fsTypeStr;
        return -1;
    }
    CURVEFS_ERROR ret = g_ClientInstance->Init(*fuseClientOption);
    if (ret != CURVEFS_ERROR::OK) {
        return -1;
    }
    ret = g_ClientInstance->Run();
    if (ret != CURVEFS_ERROR::OK) {
        return -1;
    }
    return 0;
}

void UnInitFuseClient() {
    g_ClientInstance->Fini();
    g_ClientInstance->UnInit();
    delete g_ClientInstance;
    delete fuseClientOption;
}

void FuseOpInit(void *userdata, struct fuse_conn_info *conn) {
    g_ClientInstance->FuseOpInit(userdata, conn);
}

void FuseOpDestroy(void *userdata) {
    g_ClientInstance->FuseOpDestroy(userdata);
}

void FuseReplyErrByErrCode(fuse_req_t req, CURVEFS_ERROR errcode) {
    switch (errcode) {
    case CURVEFS_ERROR::OK:
        fuse_reply_err(req, 0);
        break;
    case CURVEFS_ERROR::NO_SPACE:
        fuse_reply_err(req, ENOSPC);
        break;
    case CURVEFS_ERROR::NOTEXIST:
        fuse_reply_err(req, ENOENT);
        break;
    case CURVEFS_ERROR::NOPERMISSION:
        fuse_reply_err(req, EACCES);
        break;
    case CURVEFS_ERROR::INVALIDPARAM:
        fuse_reply_err(req, EINVAL);
        break;
    case CURVEFS_ERROR::NOTEMPTY:
        fuse_reply_err(req, ENOTEMPTY);
        break;
    case CURVEFS_ERROR::NOTSUPPORT:
        fuse_reply_err(req, ENOSYS);
        break;
    case CURVEFS_ERROR::NAMETOOLONG:
        fuse_reply_err(req, ENAMETOOLONG);
        break;
    default:
        fuse_reply_err(req, EIO);
        break;
    }
}

void FuseOpLookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
    fuse_entry_param e;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpLookup(req, parent, name, &e);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_entry(req, &e);
}

void FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info *fi) {
    struct stat attr;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpGetAttr(req, ino, fi, &attr);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_attr(req, &attr, fuseClientOption->attrTimeOut);
}

void FuseOpReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                      struct fuse_file_info *fi) {
    char *buffer = nullptr;
    size_t rSize = 0;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpReadDir(
            req, ino, size, off, fi, &buffer, &rSize);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_buf(req, buffer, rSize);
}

void FuseOpOpen(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpOpen(req, ino, fi);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_open(req, fi);
}

void FuseOpRead(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                   struct fuse_file_info *fi) {
    std::unique_ptr<char[]> buffer(new char[size]);
    memset(buffer.get(), 0, size);
    size_t rSize = 0;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpRead(
            req, ino, size, off, fi, buffer.get(), &rSize);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_buf(req, buffer.get(), rSize);
}

void FuseOpWrite(fuse_req_t req, fuse_ino_t ino, const char *buf,
                    size_t size, off_t off, struct fuse_file_info *fi) {
    size_t wSize = 0;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpWrite(req, ino, buf, size, off, fi, &wSize);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_write(req, wSize);
}

void FuseOpCreate(fuse_req_t req, fuse_ino_t parent, const char *name,
                     mode_t mode, struct fuse_file_info *fi) {
    fuse_entry_param e;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpCreate(req, parent, name, mode, fi, &e);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_create(req, &e, fi);
}

void FuseOpMkNod(fuse_req_t req, fuse_ino_t parent, const char *name,
                    mode_t mode, dev_t rdev) {
    fuse_entry_param e;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpMkNod(req, parent, name, mode, rdev, &e);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_entry(req, &e);
}

void FuseOpMkDir(fuse_req_t req, fuse_ino_t parent, const char *name,
                    mode_t mode) {
    fuse_entry_param e;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpMkDir(
        req, parent, name, mode, &e);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_entry(req, &e);
}

void FuseOpUnlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpUnlink(req, parent, name);
    FuseReplyErrByErrCode(req, ret);
}

void FuseOpRmDir(fuse_req_t req, fuse_ino_t parent, const char *name) {
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpRmDir(req, parent, name);
    FuseReplyErrByErrCode(req, ret);
}

void FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info *fi) {
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpOpenDir(req, ino, fi);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_open(req, fi);
}

void FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
            struct fuse_file_info *fi) {
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpReleaseDir(req, ino, fi);
    FuseReplyErrByErrCode(req, ret);
}

void FuseOpRename(fuse_req_t req,
                  fuse_ino_t parent,
                  const char* name,
                  fuse_ino_t newparent,
                  const char* newname,
                  unsigned int flags) {
    // TODO(Wine93): the flag RENAME_EXCHANGE and RENAME_NOREPLACE
    // is only used in linux interface renameat(), not required by posix,
    // we can ignore it now
    CURVEFS_ERROR rc;
    if (flags != 0) {
        rc = CURVEFS_ERROR::INVALIDPARAM;
    } else {
        rc = g_ClientInstance->FuseOpRename(
            req, parent, name, newparent, newname);
    }
    FuseReplyErrByErrCode(req, rc);
}

void FuseOpSetAttr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                      int to_set, struct fuse_file_info *fi) {
    struct stat attrOut;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpSetAttr(req, ino, attr, to_set, fi, &attrOut);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_attr(req, &attrOut, fuseClientOption->attrTimeOut);
}

void FuseOpSymlink(fuse_req_t req, const char *link, fuse_ino_t parent,
         const char *name) {
    fuse_entry_param e;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpSymlink(
        req, link, parent, name, &e);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_entry(req, &e);
}

void FuseOpLink(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
          const char *newname) {
    fuse_entry_param e;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpLink(
        req, ino, newparent, newname, &e);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_entry(req, &e);
}

void FuseOpReadLink(fuse_req_t req, fuse_ino_t ino) {
    std::string linkStr;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpReadLink(req, ino, &linkStr);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_readlink(req, linkStr.c_str());
}

void FuseOpRelease(fuse_req_t req, fuse_ino_t ino,
         struct fuse_file_info *fi) {
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpRelease(req, ino, fi);
    FuseReplyErrByErrCode(req, ret);
}

void FuseOpFsync(fuse_req_t req, fuse_ino_t ino, int datasync,
           struct fuse_file_info *fi) {
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpFsync(req, ino, datasync, fi);
    FuseReplyErrByErrCode(req, ret);
}
