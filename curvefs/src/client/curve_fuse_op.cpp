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
#include "curvefs/src/client/common/common.h"
#include "src/common/configuration.h"
#include "src/common/gflags_helper.h"
#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/fuse_volume_client.h"
#include "curvefs/src/client/fuse_s3_client.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/rpcclient/base_client.h"

using ::curve::common::Configuration;
using ::curvefs::client::CURVEFS_ERROR;
using ::curvefs::client::FuseClient;
using ::curvefs::client::FuseS3Client;
using ::curvefs::client::FuseVolumeClient;
using ::curvefs::client::common::FuseClientOption;
using ::curvefs::client::common::MAXXATTRLENGTH;
using ::curvefs::client::rpcclient::MdsClientImpl;
using ::curvefs::client::rpcclient::MDSBaseClient;

static FuseClient *g_ClientInstance = nullptr;
static FuseClientOption *g_fuseClientOption = nullptr;

DECLARE_int32(v);

namespace {

void EnableSplice(struct fuse_conn_info* conn) {
    if (!g_fuseClientOption->enableFuseSplice) {
        LOG(INFO) << "Fuse splice is disabled";
        return;
    }

    if (conn->capable & FUSE_CAP_SPLICE_MOVE) {
        conn->want |= FUSE_CAP_SPLICE_MOVE;
        LOG(INFO) << "FUSE_CAP_SPLICE_MOVE enabled";
    }
    if (conn->capable & FUSE_CAP_SPLICE_READ) {
        conn->want |= FUSE_CAP_SPLICE_READ;
        LOG(INFO) << "FUSE_CAP_SPLICE_READ enabled";
    }
    if (conn->capable & FUSE_CAP_SPLICE_WRITE) {
        conn->want |= FUSE_CAP_SPLICE_WRITE;
        LOG(INFO) << "FUSE_CAP_SPLICE_WRITE enabled";
    }
}

int GetFsInfo(const char* fsName, std::shared_ptr<FsInfo> fsInfo) {
    MdsClientImpl mdsClient;
    MDSBaseClient mdsBase;
    mdsClient.Init(g_fuseClientOption->mdsOpt, &mdsBase);

    std::string fn = (fsName == nullptr) ? "" : fsName;
    FSStatusCode ret = mdsClient.GetFsInfo(fn, fsInfo.get());
    if (ret != FSStatusCode::OK) {
        if (FSStatusCode::NOT_FOUND == ret) {
            LOG(ERROR) << "The fsName not exist, fsName = " << fsName;
            return -1;
        } else {
            LOG(ERROR) << "GetFsInfo failed, FSStatusCode = " << ret
                       << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                       << ", fsName = " << fsName;
            return -1;
        }
    }
    return 0;
}

}  // namespace

int InitGlog(const char *confPath, const char *argv0) {
    Configuration conf;
    conf.SetConfigPath(confPath);
    if (!conf.LoadConfig()) {
        LOG(ERROR) << "LoadConfig failed, confPath = " << confPath;
        return -1;
    }

    // set log dir
    if (FLAGS_log_dir.empty()) {
        if (!conf.GetStringValue("client.common.logDir", &FLAGS_log_dir)) {
            LOG(WARNING) << "no client.common.logDir in " << confPath
                         << ", will log to /tmp";
        }
    }

    curve::common::GflagsLoadValueFromConfIfCmdNotSet dummy;
    dummy.Load(&conf, "v", "client.loglevel", &FLAGS_v);

    // initialize logging module
    google::InitGoogleLogging(argv0);

    return 0;
}

int InitFuseClient(const char *confPath, const char* fsName,
    const char *fsType) {
    Configuration conf;
    conf.SetConfigPath(confPath);
    if (!conf.LoadConfig()) {
        LOG(ERROR) << "LoadConfig failed, confPath = " << confPath;
        return -1;
    }

    conf.PrintConfig();

    g_fuseClientOption = new FuseClientOption();
    curvefs::client::common::InitFuseClientOption(&conf, g_fuseClientOption);

    std::shared_ptr<FsInfo> fsInfo = std::make_shared<FsInfo>();
    if (GetFsInfo(fsName, fsInfo) != 0) {
        return -1;
    }

    std::string fsTypeStr = (fsType == nullptr) ? "" : fsType;
    std::string fsTypeMds;
    if (fsInfo->fstype() == FSType::TYPE_S3) {
       fsTypeMds = "s3";
    } else if (fsInfo->fstype() == FSType::TYPE_VOLUME) {
       fsTypeMds = "volume";
    }

    if (fsTypeMds != fsTypeStr) {
        LOG(ERROR) << "The parameter fstype is inconsistent with mds!";
        return -1;
    } else if (fsTypeStr == "s3") {
        g_ClientInstance = new FuseS3Client();
    } else if (fsTypeStr == "volume") {
        g_ClientInstance = new FuseVolumeClient();
    } else {
        LOG(ERROR) << "unknown fstype! fstype is " << fsTypeStr;
        return -1;
    }

    g_ClientInstance->SetFsInfo(fsInfo);
    CURVEFS_ERROR ret = g_ClientInstance->Init(*g_fuseClientOption);
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
    delete g_fuseClientOption;
}

void FuseOpInit(void *userdata, struct fuse_conn_info *conn) {
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpInit(userdata, conn);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(FATAL) << "FuseOpInit failed, ret = " << ret;
    }
    EnableSplice(conn);
    LOG(INFO) << "Fuse op init success!";
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
        fuse_reply_err(req, EOPNOTSUPP);
        break;
    case CURVEFS_ERROR::NAMETOOLONG:
        fuse_reply_err(req, ENAMETOOLONG);
        break;
    case CURVEFS_ERROR::OUT_OF_RANGE:
        fuse_reply_err(req, ERANGE);
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

void FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    struct stat attr;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpGetAttr(req, ino, fi, &attr);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_attr(req, &attr, g_fuseClientOption->attrTimeOut);
}

void FuseOpGetXattr(fuse_req_t req, fuse_ino_t ino, const char *name,
    size_t size) {
    std::unique_ptr<char[]> buf(new char[MAXXATTRLENGTH]);
    std::memset(buf.get(), 0, MAXXATTRLENGTH);
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpGetXattr(req, ino, name,
                                                         buf.get(), size);
    if (ret != CURVEFS_ERROR::OK && ret != CURVEFS_ERROR::NODATA) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }

    if (size == 0) {
        fuse_reply_xattr(req, strlen(buf.get()));
    } else {
        fuse_reply_buf(req, buf.get(), strlen(buf.get()));
    }
}

void FuseOpReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                   struct fuse_file_info *fi) {
    char *buffer = nullptr;
    size_t rSize = 0;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpReadDir(req, ino, size, off, fi,
                                                        &buffer, &rSize);
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
    size_t rSize = 0;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpRead(req, ino, size, off, fi,
                                                     buffer.get(), &rSize);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }

    struct fuse_bufvec bufvec = FUSE_BUFVEC_INIT(rSize);
    bufvec.buf[0].mem = buffer.get();

    fuse_reply_data(req, &bufvec, FUSE_BUF_SPLICE_MOVE);
}

void FuseOpWrite(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size,
                 off_t off, struct fuse_file_info *fi) {
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
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpMkDir(req, parent, name, mode, &e);
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

void FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
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

void FuseOpRename(fuse_req_t req, fuse_ino_t parent, const char *name,
                  fuse_ino_t newparent, const char *newname,
                  unsigned int flags) {
    // TODO(Wine93): the flag RENAME_EXCHANGE and RENAME_NOREPLACE
    // is only used in linux interface renameat(), not required by posix,
    // we can ignore it now
    CURVEFS_ERROR rc;
    if (flags != 0) {
        rc = CURVEFS_ERROR::INVALIDPARAM;
    } else {
        rc = g_ClientInstance->FuseOpRename(req, parent, name, newparent,
                                            newname);
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
    fuse_reply_attr(req, &attrOut, g_fuseClientOption->attrTimeOut);
}

void FuseOpSymlink(fuse_req_t req, const char *link, fuse_ino_t parent,
                   const char *name) {
    fuse_entry_param e;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpSymlink(req, link, parent, name, &e);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_entry(req, &e);
}

void FuseOpLink(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                const char *newname) {
    fuse_entry_param e;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpLink(req, ino, newparent, newname, &e);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_entry(req, &e);
}

void FuseOpReadLink(fuse_req_t req, fuse_ino_t ino) {
    std::string linkStr;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpReadLink(req, ino, &linkStr);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_readlink(req, linkStr.c_str());
}

void FuseOpRelease(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpRelease(req, ino, fi);
    FuseReplyErrByErrCode(req, ret);
}

void FuseOpFsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                 struct fuse_file_info *fi) {
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpFsync(req, ino, datasync, fi);
    FuseReplyErrByErrCode(req, ret);
}

void FuseOpFlush(fuse_req_t req, fuse_ino_t ino,
           struct fuse_file_info *fi) {
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpFlush(req, ino, fi);
    FuseReplyErrByErrCode(req, ret);
}
