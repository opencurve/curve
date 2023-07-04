/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-07-25
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_WARMUP_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_WARMUP_H_

namespace curvefs {
namespace client {
namespace filesystem {

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_WARMUP_H_


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

const char* warmupXAttr = ::curvefs::client::common::kCurveFsWarmupXAttr;

bool IsWamupReq(const char* name) {
    return strcmp(name, warmupXAttr) == 0;
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