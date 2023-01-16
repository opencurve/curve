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
#include "curvefs/src/client/metric/client_metric.h"
#include "curvefs/src/common/metric_utils.h"
#include "curvefs/src/common/dynamic_vlog.h"

using ::curve::common::Configuration;
using ::curvefs::client::CURVEFS_ERROR;
using ::curvefs::client::FuseClient;
using ::curvefs::client::FuseS3Client;
using ::curvefs::client::FuseVolumeClient;
using ::curvefs::client::common::FuseClientOption;
using ::curvefs::client::rpcclient::MdsClientImpl;
using ::curvefs::client::rpcclient::MDSBaseClient;
using ::curvefs::client::metric::ClientOpMetric;
using ::curvefs::common::LatencyUpdater;
using ::curvefs::client::metric::InflightGuard;
using ::curvefs::client::common::FileHandle;

using ::curvefs::common::FLAGS_vlog_level;

static FuseClient *g_ClientInstance = nullptr;
static FuseClientOption *g_fuseClientOption = nullptr;
static ClientOpMetric* g_clientOpMetric = nullptr;

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

int GetFsInfo(const char* fsName, FsInfo* fsInfo) {
    MdsClientImpl mdsClient;
    MDSBaseClient mdsBase;
    mdsClient.Init(g_fuseClientOption->mdsOpt, &mdsBase);

    std::string fn = (fsName == nullptr) ? "" : fsName;
    FSStatusCode ret = mdsClient.GetFsInfo(fn, fsInfo);
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
    FLAGS_vlog_level = FLAGS_v;

    // initialize logging module
    google::InitGoogleLogging(argv0);

    return 0;
}

int InitFuseClient(const char *confPath, const char* fsName,
    const char *fsType, const char *mdsAddr) {
    g_clientOpMetric = new ClientOpMetric();

    Configuration conf;
    conf.SetConfigPath(confPath);
    if (!conf.LoadConfig()) {
        LOG(ERROR) << "LoadConfig failed, confPath = " << confPath;
        return -1;
    }
    if (mdsAddr)
        conf.SetStringValue("mdsOpt.rpcRetryOpt.addrs", mdsAddr);

    conf.PrintConfig();

    g_fuseClientOption = new FuseClientOption();
    curvefs::client::common::InitFuseClientOption(&conf, g_fuseClientOption);

    std::shared_ptr<FsInfo> fsInfo = std::make_shared<FsInfo>();
    if (GetFsInfo(fsName, fsInfo.get()) != 0) {
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
    if (g_ClientInstance) {
        g_ClientInstance->Fini();
        g_ClientInstance->UnInit();
    }
    delete g_ClientInstance;
    delete g_fuseClientOption;
    delete g_clientOpMetric;
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
    case CURVEFS_ERROR::NODATA:
        fuse_reply_err(req, ENODATA);
        break;
    case CURVEFS_ERROR::EXISTS:
        fuse_reply_err(req, EEXIST);
        break;
    default:
        fuse_reply_err(req, EIO);
        break;
    }
}

void FuseOpLookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
    InflightGuard guard(&g_clientOpMetric->opLookup.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opLookup.latency);
    fuse_entry_param e;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpLookup(req, parent, name, &e);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opLookup.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_entry(req, &e);
}

void FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    InflightGuard guard(&g_clientOpMetric->opGetAttr.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opGetAttr.latency);
    struct stat attr;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpGetAttr(req, ino, fi, &attr);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opGetAttr.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_attr(req, &attr, g_fuseClientOption->attrTimeOut);
}

void FuseOpSetXattr(fuse_req_t req, fuse_ino_t ino, const char* name,
                    const char* value, size_t size, int flags) {
    std::string xattrValue(value, size);
    VLOG(9) << "FuseOpSetXattr"
            << " ino " << ino << " name " << name << " value " << xattrValue
            << " flags " << flags;
    // set xattr
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpSetXattr(req, ino, name,
        value, size, flags);
    FuseReplyErrByErrCode(req, ret);
    VLOG(9) << "FuseOpSetXattr done";
}

void FuseOpGetXattr(fuse_req_t req, fuse_ino_t ino, const char *name,
    size_t size) {
    InflightGuard guard(&g_clientOpMetric->opGetXattr.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opGetXattr.latency);
    std::string buf;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpGetXattr(req, ino, name,
                                                         &buf, size);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opGetXattr.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }

    if (size == 0) {
        fuse_reply_xattr(req, buf.length());
    } else {
        fuse_reply_buf(req, buf.data(), buf.length());
    }
}

void FuseOpListXattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
    InflightGuard guard(&g_clientOpMetric->opListXattr.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opListXattr.latency);
    std::unique_ptr<char[]> buf(new char[size]);
    std::memset(buf.get(), 0, size);
    size_t xattrSize = 0;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpListXattr(req, ino, buf.get(),
                                                          size, &xattrSize);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opListXattr.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }

    if (size == 0) {
        fuse_reply_xattr(req, xattrSize);
    } else {
        fuse_reply_buf(req, buf.get(), xattrSize);
    }
}

void FuseOpReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info *fi) {
    InflightGuard guard(&g_clientOpMetric->opReadDir.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opReadDir.latency);
    char *buffer = nullptr;
    size_t rSize = 0;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpReadDirPlus(req, ino,
        size, off, fi, &buffer, &rSize, false);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opReadDir.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_buf(req, buffer, rSize);
}

void FuseOpReadDirPlus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                       struct fuse_file_info *fi) {
    InflightGuard guard(&g_clientOpMetric->opReadDir.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opReadDir.latency);
    char *buffer = nullptr;
    size_t rSize = 0;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpReadDirPlus(req, ino,
        size, off, fi, &buffer, &rSize, true);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opReadDir.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_buf(req, buffer, rSize);
}

void FuseOpOpen(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    InflightGuard guard(&g_clientOpMetric->opOpen.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opOpen.latency);
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpOpen(req, ino, fi);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opOpen.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_open(req, fi);
}

void FuseOpRead(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                struct fuse_file_info *fi) {
    InflightGuard guard(&g_clientOpMetric->opRead.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opRead.latency);
    std::unique_ptr<char[]> buffer(new char[size]);
    size_t rSize = 0;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpRead(req, ino, size, off, fi,
                                                     buffer.get(), &rSize);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opRead.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }

    struct fuse_bufvec bufvec = FUSE_BUFVEC_INIT(rSize);
    bufvec.buf[0].mem = buffer.get();

    fuse_reply_data(req, &bufvec, FUSE_BUF_SPLICE_MOVE);
}

void FuseOpWrite(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size,
                 off_t off, struct fuse_file_info *fi) {
    InflightGuard guard(&g_clientOpMetric->opWrite.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opWrite.latency);
    size_t wSize = 0;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpWrite(req, ino, buf, size, off, fi, &wSize);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opWrite.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_write(req, wSize);
}

void FuseOpCreate(fuse_req_t req, fuse_ino_t parent, const char *name,
                  mode_t mode, struct fuse_file_info *fi) {
    InflightGuard guard(&g_clientOpMetric->opCreate.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opCreate.latency);
    fuse_entry_param e;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpCreate(req, parent, name, mode, fi, &e);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opCreate.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_create(req, &e, fi);
}

void FuseOpMkNod(fuse_req_t req, fuse_ino_t parent, const char *name,
                 mode_t mode, dev_t rdev) {
    InflightGuard guard(&g_clientOpMetric->opMkNod.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opMkNod.latency);
    fuse_entry_param e;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpMkNod(req, parent, name, mode, rdev, &e);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opMkNod.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_entry(req, &e);
}

void FuseOpMkDir(fuse_req_t req, fuse_ino_t parent, const char *name,
                 mode_t mode) {
    InflightGuard guard(&g_clientOpMetric->opMkDir.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opMkDir.latency);
    fuse_entry_param e;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpMkDir(req, parent, name, mode, &e);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opMkDir.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_entry(req, &e);
}

void FuseOpUnlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
    InflightGuard guard(&g_clientOpMetric->opUnlink.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opUnlink.latency);
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpUnlink(req, parent, name);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opUnlink.ecount << 1;
    }
    FuseReplyErrByErrCode(req, ret);
}

void FuseOpRmDir(fuse_req_t req, fuse_ino_t parent, const char *name) {
    InflightGuard guard(&g_clientOpMetric->opRmDir.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opRmDir.latency);
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpRmDir(req, parent, name);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opRmDir.ecount << 1;
    }
    FuseReplyErrByErrCode(req, ret);
}

void FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    InflightGuard guard(&g_clientOpMetric->opOpenDir.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opOpenDir.latency);
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpOpenDir(req, ino, fi);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opOpenDir.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_open(req, fi);
}

void FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info *fi) {
    InflightGuard guard(&g_clientOpMetric->opReleaseDir.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opReleaseDir.latency);
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpReleaseDir(req, ino, fi);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opReleaseDir.ecount << 1;
    }
    FuseReplyErrByErrCode(req, ret);
}

void FuseOpRename(fuse_req_t req, fuse_ino_t parent, const char *name,
                  fuse_ino_t newparent, const char *newname,
                  unsigned int flags) {
    // TODO(Wine93): the flag RENAME_EXCHANGE and RENAME_NOREPLACE
    // is only used in linux interface renameat(), not required by posix,
    // we can ignore it now
    InflightGuard guard(&g_clientOpMetric->opRename.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opRename.latency);
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;
    if (flags != 0) {
        ret = CURVEFS_ERROR::INVALIDPARAM;
    } else {
        ret = g_ClientInstance->FuseOpRename(req, parent, name, newparent,
                                            newname);
    }

    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opRename.ecount << 1;
    }
    FuseReplyErrByErrCode(req, ret);
}

void FuseOpSetAttr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                   int to_set, struct fuse_file_info *fi) {
    InflightGuard guard(&g_clientOpMetric->opSetAttr.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opSetAttr.latency);
    struct stat attrOut;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpSetAttr(req, ino, attr, to_set, fi, &attrOut);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opSetAttr.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_attr(req, &attrOut, g_fuseClientOption->attrTimeOut);
}

void FuseOpSymlink(fuse_req_t req, const char *link, fuse_ino_t parent,
                   const char *name) {
    InflightGuard guard(&g_clientOpMetric->opSymlink.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opSymlink.latency);
    fuse_entry_param e;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpSymlink(req, link, parent, name, &e);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opSymlink.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_entry(req, &e);
}

void FuseOpLink(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                const char *newname) {
    InflightGuard guard(&g_clientOpMetric->opLink.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opLink.latency);
    fuse_entry_param e;
    CURVEFS_ERROR ret =
        g_ClientInstance->FuseOpLink(req, ino, newparent, newname, &e);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opLink.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_entry(req, &e);
}

void FuseOpReadLink(fuse_req_t req, fuse_ino_t ino) {
    InflightGuard guard(&g_clientOpMetric->opReadLink.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opReadLink.latency);
    std::string linkStr;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpReadLink(req, ino, &linkStr);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opReadLink.ecount << 1;
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_readlink(req, linkStr.c_str());
}

void FuseOpRelease(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    InflightGuard guard(&g_clientOpMetric->opRelease.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opRelease.latency);
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpRelease(req, ino, fi);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opRelease.ecount << 1;
    }
    FuseReplyErrByErrCode(req, ret);
}

void FuseOpFsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                 struct fuse_file_info *fi) {
    InflightGuard guard(&g_clientOpMetric->opFsync.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opFsync.latency);
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpFsync(req, ino, datasync, fi);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opFsync.ecount << 1;
    }
    FuseReplyErrByErrCode(req, ret);
}

void FuseOpFlush(fuse_req_t req, fuse_ino_t ino,
           struct fuse_file_info *fi) {
    InflightGuard guard(&g_clientOpMetric->opFlush.inflightOpNum);
    LatencyUpdater updater(&g_clientOpMetric->opFlush.latency);
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpFlush(req, ino, fi);
    if (ret != CURVEFS_ERROR::OK) {
        g_clientOpMetric->opFlush.ecount << 1;
    }
    FuseReplyErrByErrCode(req, ret);
}

void FuseOpBmap(fuse_req_t req,
                fuse_ino_t /*ino*/,
                size_t /*blocksize*/,
                uint64_t /*idx*/) {
    // TODO(wuhanqing): implement for volume storage
    FuseReplyErrByErrCode(req, CURVEFS_ERROR::NOTSUPPORT);
}

void FuseOpStatFs(fuse_req_t req, fuse_ino_t ino) {
    struct statvfs stbuf;
    CURVEFS_ERROR ret = g_ClientInstance->FuseOpStatFs(req, ino, &stbuf);
    if (ret != CURVEFS_ERROR::OK) {
        FuseReplyErrByErrCode(req, ret);
        return;
    }
    fuse_reply_statfs(req, &stbuf);
}
