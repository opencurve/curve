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
#include <cstring>
#include <utility>
#include <vector>

#include "curvefs/src/client/curve_fuse_op.h"
#include "curvefs/src/client/fuse_client.h"
#include "curvefs/src/client/filesystem/error.h"
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
#include "curvefs/src/client/warmup/warmup_manager.h"
#include "curvefs/src/client/filesystem/meta.h"
#include "curvefs/src/client/filesystem/access_log.h"
#include "curvefs/src/client/filesystem/metric.h"
#include "curvefs/src/client/filesystem/xattr.h"

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
using ::curvefs::client::filesystem::EntryOut;
using ::curvefs::client::filesystem::AttrOut;
using ::curvefs::client::filesystem::FileOut;
using ::curvefs::client::filesystem::AccessLogGuard;
using ::curvefs::client::filesystem::StrFormat;
using ::curvefs::client::filesystem::InitAccessLog;
using ::curvefs::client::filesystem::Logger;
using ::curvefs::client::filesystem::StrEntry;
using ::curvefs::client::filesystem::StrAttr;
using ::curvefs::client::filesystem::StrMode;
using ::curvefs::client::filesystem::IsWarmupXAttr;

using ::curvefs::common::FLAGS_vlog_level;

static FuseClient *g_ClientInstance = nullptr;
static FuseClientOption *g_fuseClientOption = nullptr;
static ClientOpMetric* g_clientOpMetric = nullptr;

namespace {

void EnableCaps(struct fuse_conn_info* conn) {
    // enable acl
    if (g_fuseClientOption->enableACL &&
        conn->capable & FUSE_CAP_POSIX_ACL) {
        conn->want |= FUSE_CAP_POSIX_ACL;
        LOG(INFO) << "FUSE_CAP_POSIX_ACL enabled";
    }

    // enable splice
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

int InitLog(const char *confPath, const char *argv0) {
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

    bool succ = InitAccessLog(FLAGS_log_dir);
    if (!succ) {
        return -1;
    }
    return 0;
}

int InitFuseClient(const struct MountOption *mountOption) {
    g_clientOpMetric = new ClientOpMetric();

    Configuration conf;
    conf.SetConfigPath(mountOption->conf);
    if (!conf.LoadConfig()) {
        LOG(ERROR) << "LoadConfig failed, confPath = " << mountOption->conf;
        return -1;
    }
    if (mountOption->mdsAddr)
        conf.SetStringValue("mdsOpt.rpcRetryOpt.addrs", mountOption->mdsAddr);

    conf.PrintConfig();

    g_fuseClientOption = new FuseClientOption();
    curvefs::client::common::InitFuseClientOption(&conf, g_fuseClientOption);

    std::shared_ptr<FsInfo> fsInfo = std::make_shared<FsInfo>();
    if (GetFsInfo(mountOption->fsName, fsInfo.get()) != 0) {
        return -1;
    }

    std::string fsTypeStr =
        (mountOption->fsType == nullptr) ? "" : mountOption->fsType;
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

    ret = g_ClientInstance->SetMountStatus(mountOption);
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

int AddWarmupTask(curvefs::client::common::WarmupType type, fuse_ino_t key,
                  const std::string &path,
                  curvefs::client::common::WarmupStorageType storageType) {
    int ret = 0;
    bool result = true;
    switch (type) {
    case curvefs::client::common::WarmupType::kWarmupTypeList:
        result = g_ClientInstance->PutWarmFilelistTask(key, storageType);
        break;
    case curvefs::client::common::WarmupType::kWarmupTypeSingle:
        result = g_ClientInstance->PutWarmFileTask(key, path, storageType);
        break;
    default:
        // not support add warmup type (warmup single file/dir or filelist)
        LOG(ERROR) << "not support warmup type, only support single/list";
        ret = EOPNOTSUPP;
    }
    if (!result) {
        ret = ERANGE;
    }
    return ret;
}

void QueryWarmupTask(fuse_ino_t key, std::string *data) {
    curvefs::client::warmup::WarmupProgress progress;
    bool ret = g_ClientInstance->GetWarmupProgress(key, &progress);
    if (!ret) {
        *data = "finished";
    } else {
        *data = std::to_string(progress.GetFinished()) + "/" +
                std::to_string(progress.GetTotal());
    }
    VLOG(9) << "Warmup [" << key << "]" << *data;
}

int Warmup(fuse_ino_t key, const std::string& name, const std::string& value) {
    // warmup
    if (g_ClientInstance->GetFsInfo()->fstype() != FSType::TYPE_S3) {
        LOG(ERROR) << "warmup only support s3";
        return EOPNOTSUPP;
    }

    std::vector<std::string> opTypePath;
    curve::common::SplitString(value, "\n", &opTypePath);
    if (opTypePath.size() != curvefs::client::common::kWarmupOpNum) {
        LOG(ERROR) << name << " has invalid xattr value " << value;
        return ERANGE;
    }
    auto storageType =
        curvefs::client::common::GetWarmupStorageType(opTypePath[3]);
    if (storageType ==
        curvefs::client::common::WarmupStorageType::kWarmupStorageTypeUnknown) {
        LOG(ERROR) << name << " not support storage type: " << value;
        return ERANGE;
    }
    int ret = 0;
    switch (curvefs::client::common::GetWarmupOpType(opTypePath[0])) {
    case curvefs::client::common::WarmupOpType::kWarmupOpAdd:
        ret =
            AddWarmupTask(curvefs::client::common::GetWarmupType(opTypePath[1]),
                          key, opTypePath[2], storageType);
        if (ret != 0) {
            LOG(ERROR) << name << " has invalid xattr value " << value;
        }
        break;
    default:
        LOG(ERROR) << name << " has invalid xattr value " << value;
        ret = ERANGE;
    }
    return ret;
}

namespace {

struct CodeGuard {
    explicit CodeGuard(CURVEFS_ERROR* rc, bvar::Adder<uint64_t>* ecount)
    : rc_(rc), ecount_(ecount) {}

    ~CodeGuard() {
        if (*rc_ != CURVEFS_ERROR::OK) {
            (*ecount_) << 1;
        }
    }

    CURVEFS_ERROR* rc_;
    bvar::Adder<uint64_t>* ecount_;
};

FuseClient* Client() {
    return g_ClientInstance;
}

void TriggerWarmup(fuse_req_t req,
                   fuse_ino_t ino,
                   const char* name,
                   const char* value,
                   size_t size) {
    auto fs = Client()->GetFileSystem();

    std::string xattr(value, size);
    int code = Warmup(ino, name, xattr);
    fuse_reply_err(req, code);
}

void QueryWarmup(fuse_req_t req, fuse_ino_t ino, size_t size) {
    auto fs = Client()->GetFileSystem();

    std::string data;
    QueryWarmupTask(ino, &data);
    if (size == 0) {
        return fs->ReplyXattr(req, data.length());
    }
    return fs->ReplyBuffer(req, data.data(), data.length());
}

void ReadThrottleAdd(size_t size) { Client()->Add(true, size); }
void WriteThrottleAdd(size_t size) { Client()->Add(false, size); }

#define MetricGuard(REQUEST) \
    InflightGuard iGuard(&g_clientOpMetric->op##REQUEST.inflightOpNum); \
    CodeGuard cGuard(&rc, &g_clientOpMetric->op##REQUEST.ecount); \
    LatencyUpdater updater(&g_clientOpMetric->op##REQUEST.latency)

}  // namespace

void FuseOpInit(void *userdata, struct fuse_conn_info *conn) {
    CURVEFS_ERROR rc;
    auto client = Client();
    AccessLogGuard log([&](){
        return StrFormat("init : %s", StrErr(rc));
    });

    rc = client->FuseOpInit(userdata, conn);
    if (rc != CURVEFS_ERROR::OK) {
        LOG(FATAL) << "FuseOpInit() failed, retCode = " << rc;
    } else {
        EnableCaps(conn);
        LOG(INFO) << "FuseOpInit() success, retCode = " << rc;
    }
}

void FuseOpDestroy(void *userdata) {
    auto client = Client();
    AccessLogGuard log([&](){
        return StrFormat("destory : OK");
    });
    client->FuseOpDestroy(userdata);
}

void FuseOpLookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
    CURVEFS_ERROR rc;
    EntryOut entryOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Lookup);
    AccessLogGuard log([&](){
        return StrFormat("lookup (%d,%s): %s%s",
                         parent, name, StrErr(rc), StrEntry(entryOut));
    });

    rc = client->FuseOpLookup(req, parent, name, &entryOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyEntry(req, &entryOut);
}

void FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    AttrOut attrOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(GetAttr);
    AccessLogGuard log([&](){
        return StrFormat("getattr (%d): %s%s",
                         ino, StrErr(rc), StrAttr(attrOut));
    });

    rc = client->FuseOpGetAttr(req, ino, fi, &attrOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyAttr(req, &attrOut);
}

void FuseOpSetAttr(fuse_req_t req,
                   fuse_ino_t ino,
                   struct stat* attr,
                   int to_set,
                   struct fuse_file_info* fi) {
    CURVEFS_ERROR rc;
    AttrOut attrOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(SetAttr);
    AccessLogGuard log([&](){
        return StrFormat("setattr (%d,0x%X): %s%s",
                         ino, to_set, StrErr(rc), StrAttr(attrOut));
    });

    rc = client->FuseOpSetAttr(req, ino, attr, to_set, fi, &attrOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyAttr(req, &attrOut);
}

void FuseOpReadLink(fuse_req_t req, fuse_ino_t ino) {
    CURVEFS_ERROR rc;
    std::string link;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(ReadLink);
    AccessLogGuard log([&](){
        return StrFormat("readlink (%d): %s %s", ino, StrErr(rc), link.c_str());
    });

    rc = client->FuseOpReadLink(req, ino, &link);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyReadlink(req, link);
}

void FuseOpMkNod(fuse_req_t req,
                 fuse_ino_t parent,
                 const char* name,
                 mode_t mode,
                 dev_t rdev) {
    CURVEFS_ERROR rc;
    EntryOut entryOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(MkNod);
    AccessLogGuard log([&](){
        return StrFormat("mknod (%d,%s,%s:0%04o): %s%s",
                         parent, name, StrMode(mode), mode,
                         StrErr(rc), StrEntry(entryOut));
    });

    rc = client->FuseOpMkNod(req, parent, name, mode, rdev, &entryOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyEntry(req, &entryOut);
}

void FuseOpMkDir(fuse_req_t req,
                 fuse_ino_t parent,
                 const char* name,
                 mode_t mode) {
    CURVEFS_ERROR rc;
    EntryOut entryOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(MkDir);
    AccessLogGuard log([&](){
        return StrFormat("mkdir (%d,%s,%s:0%04o): %s%s",
                         parent, name, StrMode(mode), mode,
                         StrErr(rc), StrEntry(entryOut));
    });

    rc = client->FuseOpMkDir(req, parent, name, mode, &entryOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyEntry(req, &entryOut);
}

void FuseOpUnlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Unlink);
    AccessLogGuard log([&](){
        return StrFormat("unlink (%d,%s): %s", parent, name, StrErr(rc));
    });

    rc = client->FuseOpUnlink(req, parent, name);
    return fs->ReplyError(req, rc);
}

void FuseOpRmDir(fuse_req_t req, fuse_ino_t parent, const char *name) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(RmDir);
    AccessLogGuard log([&](){
        return StrFormat("rmdir (%d,%s): %s", parent, name, StrErr(rc));
    });

    rc = client->FuseOpRmDir(req, parent, name);
    return fs->ReplyError(req, rc);
}

void FuseOpSymlink(fuse_req_t req,
                   const char *link,
                   fuse_ino_t parent,
                   const char* name) {
    CURVEFS_ERROR rc;
    EntryOut entryOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Symlink);
    AccessLogGuard log([&](){
        return StrFormat("symlink (%d,%s,%s): %s%s",
                         parent, name, link, StrErr(rc), StrEntry(entryOut));
    });

    rc = client->FuseOpSymlink(req, link, parent, name, &entryOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyEntry(req, &entryOut);
}

void FuseOpRename(fuse_req_t req,
                  fuse_ino_t parent,
                  const char *name,
                  fuse_ino_t newparent,
                  const char *newname,
                  unsigned int flags) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Rename);
    AccessLogGuard log([&](){
        return StrFormat("rename (%d,%s,%d,%s,%d): %s",
                         parent, name, newparent, newname, flags, StrErr(rc));
    });

    rc = client->FuseOpRename(req, parent, name, newparent, newname, flags);
    return fs->ReplyError(req, rc);
}

void FuseOpLink(fuse_req_t req,
                fuse_ino_t ino,
                fuse_ino_t newparent,
                const char *newname) {
    CURVEFS_ERROR rc;
    EntryOut entryOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Link);
    AccessLogGuard log([&](){
        return StrFormat(
            "link (%d,%d,%s): %s%s",
            ino, newparent, newname, StrErr(rc), StrEntry(entryOut));
    });

    rc = client->FuseOpLink(req, ino, newparent, newname, &entryOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyEntry(req, &entryOut);
}

void FuseOpOpen(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    FileOut fileOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Open);
    AccessLogGuard log([&](){
        return StrFormat("open (%d): %s [fh:%d]", ino, StrErr(rc), fi->fh);
    });

    rc = client->FuseOpOpen(req, ino, fi, &fileOut);
    if (rc != CURVEFS_ERROR::OK) {
        fs->ReplyError(req, rc);
        return;
    }
    return fs->ReplyOpen(req, &fileOut);
}

void FuseOpRead(fuse_req_t req,
                fuse_ino_t ino,
                size_t size,
                off_t off,
                struct fuse_file_info* fi) {
    CURVEFS_ERROR rc;
    size_t rSize = 0;
    std::unique_ptr<char[]> buffer(new char[size]);
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Read);
    AccessLogGuard log([&](){
        return StrFormat("read (%d,%d,%d,%d): %s (%d)",
                         ino, size, off, fi->fh, StrErr(rc), rSize);
    });

    ReadThrottleAdd(size);
    rc = client->FuseOpRead(req, ino, size, off, fi, buffer.get(), &rSize);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    struct fuse_bufvec bufvec = FUSE_BUFVEC_INIT(rSize);
    bufvec.buf[0].mem = buffer.get();
    return fs->ReplyData(req, &bufvec, FUSE_BUF_SPLICE_MOVE);
}

void FuseOpWrite(fuse_req_t req,
                 fuse_ino_t ino,
                 const char* buf,
                 size_t size,
                 off_t off,
                 struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    FileOut fileOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Write);
    AccessLogGuard log([&](){
        return StrFormat("write (%d,%d,%d,%d): %s (%d)",
                         ino, size, off, fi->fh, StrErr(rc), fileOut.nwritten);
    });

    WriteThrottleAdd(size);
    rc = client->FuseOpWrite(req, ino, buf, size, off, fi, &fileOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyWrite(req, &fileOut);
}

void FuseOpFlush(fuse_req_t req,
                 fuse_ino_t ino,
                 struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Flush);
    AccessLogGuard log([&](){
        return StrFormat("flush (%d,%d): %s", ino, fi->fh, StrErr(rc));
    });

    rc = client->FuseOpFlush(req, ino, fi);
    return fs->ReplyError(req, rc);
}

void FuseOpRelease(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Release);
    AccessLogGuard log([&](){
        return StrFormat("release (%d,%d): %s", ino, fi->fh, StrErr(rc));
    });

    rc = client->FuseOpRelease(req, ino, fi);
    return fs->ReplyError(req, rc);
}

void FuseOpFsync(fuse_req_t req,
                 fuse_ino_t ino,
                 int datasync,
                 struct fuse_file_info* fi) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Fsync);
    AccessLogGuard log([&](){
        return StrFormat("fsync (%d,%d): %s", ino, datasync, StrErr(rc));
    });

    rc = client->FuseOpFsync(req, ino, datasync, fi);
    return fs->ReplyError(req, rc);
}

void FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(OpenDir);
    AccessLogGuard log([&](){
        return StrFormat("opendir (%d): %s [fh:%d]", ino, StrErr(rc), fi->fh);
    });

    rc = client->FuseOpOpenDir(req, ino, fi);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyOpen(req, fi);
}

void FuseOpReadDir(fuse_req_t req,
                   fuse_ino_t ino,
                   size_t size,
                   off_t off,
                   struct fuse_file_info* fi) {
    CURVEFS_ERROR rc;
    char *buffer;
    size_t rSize;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(ReadDir);
    AccessLogGuard log([&](){
        return StrFormat("readdir (%d,%d,%d): %s (%d)",
                         ino, size, off, StrErr(rc), rSize);
    });

    rc = client->FuseOpReadDir(req, ino, size, off, fi, &buffer, &rSize, false);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyBuffer(req, buffer, rSize);
}

void FuseOpReadDirPlus(fuse_req_t req,
                       fuse_ino_t ino,
                       size_t size,
                       off_t off,
                       struct fuse_file_info* fi) {
    CURVEFS_ERROR rc;
    char *buffer;
    size_t rSize;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(ReadDir);
    AccessLogGuard log([&](){
        return StrFormat("readdirplus (%d,%d,%d): %s (%d)",
                         ino, size, off, StrErr(rc), rSize);
    });

    rc = client->FuseOpReadDir(req, ino, size, off, fi, &buffer, &rSize, true);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }

    return fs->ReplyBuffer(req, buffer, rSize);
}

void FuseOpReleaseDir(fuse_req_t req,
                      fuse_ino_t ino,
                      struct fuse_file_info *fi) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(ReleaseDir);
    AccessLogGuard log([&](){
        return StrFormat("releasedir (%d,%d): %s", ino, fi->fh, StrErr(rc));
    });

    rc = client->FuseOpReleaseDir(req, ino, fi);
    return fs->ReplyError(req, rc);
}

void FuseOpStatFs(fuse_req_t req, fuse_ino_t ino) {
    CURVEFS_ERROR rc;
    struct statvfs stbuf;
    auto client = Client();
    auto fs = client->GetFileSystem();
    AccessLogGuard log([&](){
        return StrFormat("statfs (%d): %s", ino, StrErr(rc));
    });

    rc = client->FuseOpStatFs(req, ino, &stbuf);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyStatfs(req, &stbuf);
}

void FuseOpSetXattr(fuse_req_t req,
                    fuse_ino_t ino,
                    const char* name,
                    const char* value,
                    size_t size,
                    int flags) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(SetXattr);
    AccessLogGuard log([&](){
        return StrFormat("setxattr (%d,%s,%d,%d): %s",
                         ino, name, size, flags, StrErr(rc));
    });

    // FIXME(Wine93): please handle it in FuseClient.
    if (IsWarmupXAttr(name)) {
        return TriggerWarmup(req, ino, name, value, size);
    }
    rc = client->FuseOpSetXattr(req, ino, name, value, size, flags);
    return fs->ReplyError(req, rc);
}

void FuseOpGetXattr(fuse_req_t req,
                    fuse_ino_t ino,
                    const char *name,
                    size_t size) {
    CURVEFS_ERROR rc;
    std::string value;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(GetXattr);
    AccessLogGuard log([&](){
        return StrFormat("getxattr (%d,%s,%d): %s (%d)",
                         ino, name, size, StrErr(rc), value.size());
    });

    // FIXME(Wine93): please handle it in FuseClient.
    if (IsWarmupXAttr(name)) {
        return QueryWarmup(req, ino, size);
    }

    rc = Client()->FuseOpGetXattr(req, ino, name, &value, size);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    } else if (size == 0) {
        return fs->ReplyXattr(req, value.length());
    }
    return fs->ReplyBuffer(req, value.data(), value.length());
}

void FuseOpListXattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
    CURVEFS_ERROR rc;
    size_t xattrSize = 0;
    std::unique_ptr<char[]> buf(new char[size]);
    std::memset(buf.get(), 0, size);
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(ListXattr);
    AccessLogGuard log([&](){
        return StrFormat("listxattr (%d,%s): %s (%d)",
                         ino, size, StrErr(rc), xattrSize);
    });

    rc = Client()->FuseOpListXattr(req, ino, buf.get(), size, &xattrSize);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    } else if (size == 0) {
        return fs->ReplyXattr(req, xattrSize);
    }
    return fs->ReplyBuffer(req, buf.get(), xattrSize);
}

void FuseOpRemoveXattr(fuse_req_t req, fuse_ino_t ino, const char *name) {
    CURVEFS_ERROR rc;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(RemoveXattr);
    AccessLogGuard log([&](){
        return StrFormat("removexattr (%d,%s): %s", ino, name, StrErr(rc));
    });

    rc = client->FuseOpRemoveXattr(req, ino, name);
    return fs->ReplyError(req, rc);
}

void FuseOpCreate(fuse_req_t req,
                  fuse_ino_t parent,
                  const char* name,
                  mode_t mode,
                  struct fuse_file_info* fi) {
    CURVEFS_ERROR rc;
    EntryOut entryOut;
    auto client = Client();
    auto fs = client->GetFileSystem();
    MetricGuard(Create);
    AccessLogGuard log([&](){
        return StrFormat("create (%d,%s): %s%s [fh:%d]",
                         parent, name, StrErr(rc), StrEntry(entryOut), fi->fh);
    });

    rc = client->FuseOpCreate(req, parent, name, mode, fi, &entryOut);
    if (rc != CURVEFS_ERROR::OK) {
        return fs->ReplyError(req, rc);
    }
    return fs->ReplyCreate(req, &entryOut, fi);
}

void FuseOpBmap(fuse_req_t req,
                fuse_ino_t /*ino*/,
                size_t /*blocksize*/,
                uint64_t /*idx*/) {
    // TODO(wuhanqing): implement for volume storage
    auto client = Client();
    auto fs = client->GetFileSystem();

    return fs->ReplyError(req, CURVEFS_ERROR::NOTSUPPORT);
}
