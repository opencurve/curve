/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: nebd
 * Created Date: 2020-02-03
 * Author: lixiaocui
 */

#include "nebd/src/part2/request_executor_curve.h"

#include <glog/logging.h>

#include "nebd/proto/client.pb.h"

namespace nebd {
namespace server {

using ::curve::client::UserInfo_t;

const char* kSessionAttrKey = "session";
const char* kOpenFlagsAttrKey = "openflags";

curve::client::OpenFlags ConverToCurveOpenFlags(
    const OpenFlags* flags, const std::string& confPath) {
    curve::client::OpenFlags curveflags;
    curveflags.confPath = confPath;
    if (!flags) {
        return curveflags;
    } else {
        if (flags->has_exclusive()) {
            curveflags.exclusive = flags->exclusive();
        }
        return curveflags;
    }
}

std::pair<std::string, std::string>
FileNameParser::Parse(const std::string& fileName) {
    std::string confPath;
    auto beginPos = fileName.find_first_of("/");
    if (beginPos == std::string::npos) {
        LOG(ERROR) << "error format fileName: " << fileName;
        return std::make_pair("", "");
    }
    beginPos += 1;

    auto endPos = fileName.find_last_of(":");
    if (endPos == std::string::npos) {
        LOG(ERROR) << "error format fileName: " << fileName;
        return std::make_pair("", "");
    }

    if (endPos < beginPos) {
        endPos = fileName.length();
    } else if (endPos < fileName.length() - 1) {
        confPath = fileName.substr(endPos + 1);
    }

    if (beginPos >= endPos) {
        LOG(ERROR) << "error format fileName: " << fileName;
        return std::make_pair("", "");
    }

    auto length = endPos - beginPos;
    if (length <= 2) {
        LOG(ERROR) << "error format fileName: " << fileName;
        return std::make_pair("", "");
    }

    return std::make_pair(fileName.substr(beginPos, length), confPath);
}

void CurveRequestExecutor::Init(const std::shared_ptr<CurveClient> &client) {
    client_ = client;
}

std::shared_ptr<NebdFileInstance> CurveRequestExecutor::Open(
    const std::string& filename, const OpenFlags* openFlags) {
    auto curveFileInfo = FileNameParser::Parse(filename);
    if (curveFileInfo.first.empty()) {
        return nullptr;
    }

    int fd = client_->Open(
        curveFileInfo.first,
        ConverToCurveOpenFlags(openFlags, curveFileInfo.second));

    if (fd >= 0) {
        auto curveFileInstance = std::make_shared<CurveFileInstance>();
        curveFileInstance->fd = fd;
        curveFileInstance->fileName = curveFileInfo.first;
        curveFileInstance->xattr[kSessionAttrKey] = "";

        if (openFlags) {
            curveFileInstance->xattr[kOpenFlagsAttrKey] =
                openFlags->SerializeAsString();
        }

        return curveFileInstance;
    }

    return nullptr;
}

std::shared_ptr<NebdFileInstance>
CurveRequestExecutor::Reopen(const std::string& filename,
                             const ExtendAttribute& xattr) {
    auto curveFileInfo = FileNameParser::Parse(filename);
    if (curveFileInfo.first.empty()) {
        return nullptr;
    }

    std::string newSessionId;
    std::string oldSessionId = xattr.at(kSessionAttrKey);

    OpenFlags flags;
    if (xattr.count(kOpenFlagsAttrKey)) {
        if (!flags.ParseFromString(xattr.at(kOpenFlagsAttrKey))) {
            LOG(ERROR) << "Parse openflags failed";
            return nullptr;
        }
    }

    int fd = client_->ReOpen(
        curveFileInfo.first,
        ConverToCurveOpenFlags(&flags, curveFileInfo.second));
    if (fd >= 0) {
        auto curveFileInstance = std::make_shared<CurveFileInstance>();
        curveFileInstance->fd = fd;
        curveFileInstance->fileName = curveFileInfo.first;
        curveFileInstance->xattr[kSessionAttrKey] = newSessionId;
        if (xattr.count(kOpenFlagsAttrKey)) {
            curveFileInstance->xattr[kOpenFlagsAttrKey] =
                xattr.at(kOpenFlagsAttrKey);
        }
        return curveFileInstance;
    }

    return nullptr;
}

int CurveRequestExecutor::Close(NebdFileInstance* fd) {
    int curveFd = GetCurveFdFromNebdFileInstance(fd);
    if (curveFd < 0) {
        return -1;
    }

    int res = client_->Close(curveFd);
    if (res != LIBCURVE_ERROR::OK) {
        return -1;
    }

    return 0;
}

int CurveRequestExecutor::Extend(NebdFileInstance* fd, int64_t newsize) {
    std::string fileName = GetFileNameFromNebdFileInstance(fd);
    if (fileName.empty()) {
        return -1;
    }

    int res = client_->Extend(fileName, newsize);
    if (res != LIBCURVE_ERROR::OK) {
        return -1;
    }

    return 0;
}

int CurveRequestExecutor::GetInfo(
    NebdFileInstance* fd, NebdFileInfo* fileInfo) {
    int curveFd = GetCurveFdFromNebdFileInstance(fd);
    if (curveFd < 0) {
        LOG(ERROR) << "Parse curve fd failed";
        return -1;
    }

    FileStatInfo statInfo;
    int64_t rc = client_->StatFile(curveFd, &statInfo);
    if (rc < 0) {
        return -1;
    }

    fileInfo->size = statInfo.length;
    fileInfo->block_size = statInfo.blocksize;
    return 0;
}

int CurveRequestExecutor::Discard(NebdFileInstance* fd,
                                  NebdServerAioContext* aioctx) {
    int curveFd = GetCurveFdFromNebdFileInstance(fd);
    if (curveFd < 0) {
        LOG(ERROR) << "Parse curve fd failed";
        return -1;
    }

    CurveAioCombineContext* curveCombineCtx = new CurveAioCombineContext();
    curveCombineCtx->nebdCtx = aioctx;
    int ret = FromNebdCtxToCurveCtx(aioctx, &curveCombineCtx->curveCtx);
    if (ret < 0) {
        LOG(ERROR) << "Convert nebd aio context to curve aio context failed, "
                      "curve fd: "
                   << curveFd;
        delete curveCombineCtx;
        return -1;
    }

    ret = client_->AioDiscard(curveFd, &curveCombineCtx->curveCtx);
    if (ret == LIBCURVE_ERROR::OK) {
        return 0;
    }

    LOG(ERROR) << "Curve client return failed, curve fd: " << curveFd;
    delete curveCombineCtx;
    return -1;
}

int CurveRequestExecutor::AioRead(
    NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    int curveFd = GetCurveFdFromNebdFileInstance(fd);
    if (curveFd < 0) {
        return -1;
    }

    CurveAioCombineContext *curveCombineCtx = new CurveAioCombineContext();
    curveCombineCtx->nebdCtx = aioctx;
    int ret = FromNebdCtxToCurveCtx(aioctx, &curveCombineCtx->curveCtx);
    if (ret < 0) {
        delete curveCombineCtx;
        return -1;
    }

    ret = client_->AioRead(curveFd, &curveCombineCtx->curveCtx,
                           curve::client::UserDataType::IOBuffer);
    if (ret !=  LIBCURVE_ERROR::OK) {
        delete curveCombineCtx;
        return -1;
    }

    return 0;
}

int CurveRequestExecutor::AioWrite(
    NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    int curveFd = GetCurveFdFromNebdFileInstance(fd);
    if (curveFd < 0) {
        return -1;
    }

    CurveAioCombineContext *curveCombineCtx = new CurveAioCombineContext();
    curveCombineCtx->nebdCtx = aioctx;
    int ret = FromNebdCtxToCurveCtx(aioctx, &curveCombineCtx->curveCtx);
    if (ret < 0) {
        delete curveCombineCtx;
        return -1;
    }

    ret = client_->AioWrite(curveFd, &curveCombineCtx->curveCtx,
                            curve::client::UserDataType::IOBuffer);
    if (ret !=  LIBCURVE_ERROR::OK) {
        delete curveCombineCtx;
        return -1;
    }

    return 0;
}

int CurveRequestExecutor::Flush(
    NebdFileInstance* fd, NebdServerAioContext* aioctx) {
    (void)fd;

    aioctx->ret = 0;
    aioctx->cb(aioctx);

    return 0;
}

int CurveRequestExecutor::InvalidCache(NebdFileInstance* fd) {
    auto curveFileInstance = dynamic_cast<CurveFileInstance *>(fd);
    if (curveFileInstance == nullptr ||
        curveFileInstance->fd < 0 ||
        curveFileInstance->fileName.empty()) {
        return -1;
    }

    return 0;
}

int CurveRequestExecutor::GetCurveFdFromNebdFileInstance(NebdFileInstance* fd) {
    auto curveFileInstance = dynamic_cast<CurveFileInstance *>(fd);
    if (curveFileInstance == nullptr) {
        return -1;
    }

    return curveFileInstance->fd;
}

std::string CurveRequestExecutor::GetFileNameFromNebdFileInstance(
    NebdFileInstance* fd) {
    auto curveFileInstance = dynamic_cast<CurveFileInstance *>(fd);
    if (curveFileInstance == nullptr) {
        return "";
    }

    return curveFileInstance->fileName;
}

int CurveRequestExecutor::FromNebdCtxToCurveCtx(
        NebdServerAioContext *nebdCtx, CurveAioContext *curveCtx) {
    curveCtx->offset = nebdCtx->offset;
    curveCtx->length = nebdCtx->size;
    int ret = FromNebdOpToCurveOp(nebdCtx->op, &curveCtx->op);
    if (ret < 0) {
        return -1;
    }
    curveCtx->buf = nebdCtx->buf;
    curveCtx->cb = CurveAioCallback;
    return 0;
}

int CurveRequestExecutor::FromNebdOpToCurveOp(LIBAIO_OP op, LIBCURVE_OP *out) {
    switch (op) {
    case LIBAIO_OP::LIBAIO_OP_READ:
        *out = LIBCURVE_OP::LIBCURVE_OP_READ;
        return 0;
    case LIBAIO_OP::LIBAIO_OP_WRITE:
        *out = LIBCURVE_OP_WRITE;
        return 0;
    case LIBAIO_OP::LIBAIO_OP_DISCARD:
        *out = LIBCURVE_OP_DISCARD;
        return 0;
    default:
        return -1;
    }
}

void CurveAioCallback(struct CurveAioContext* curveCtx) {
    auto curveCombineCtx = reinterpret_cast<CurveAioCombineContext *>(
        reinterpret_cast<char *>(curveCtx) -
        offsetof(CurveAioCombineContext, curveCtx));
    curveCombineCtx->nebdCtx->ret = curveCtx->ret;
    curveCombineCtx->nebdCtx->cb(curveCombineCtx->nebdCtx);
    delete curveCombineCtx;
}

}  // namespace server
}  // namespace nebd
