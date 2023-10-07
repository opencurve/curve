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
 * Created Date: 2023-03-06
 * Author: Jingli Chen (Wine93)
 */

#include <map>
#include <utility>
#include <ostream>

#include "curvefs/src/client/filesystem/error.h"

namespace curvefs {
namespace client {
namespace filesystem {

static const std::map<CURVEFS_ERROR, std::pair<int, std::string>> errors = {
    { CURVEFS_ERROR::OK, { 0, "OK"} },
    { CURVEFS_ERROR::INTERNAL,  { EIO, "internal error" } },
    { CURVEFS_ERROR::UNKNOWN,  { -1, "unknown" } },
    { CURVEFS_ERROR::EXISTS,  { EEXIST, "inode or dentry already exist" } },
    { CURVEFS_ERROR::NOT_EXIST, { ENOENT, "inode or dentry not exist" } },
    { CURVEFS_ERROR::NO_SPACE,  { ENOSPC, "no space to alloc" } },
    { CURVEFS_ERROR::BAD_FD,  { EBADF, "bad file number" } },
    { CURVEFS_ERROR::INVALID_PARAM , { EINVAL , "invalid argument" } },
    { CURVEFS_ERROR::NO_PERMISSION,  { EACCES, "permission denied" } },
    { CURVEFS_ERROR::NOT_EMPTY, { ENOTEMPTY, "directory not empty" } },
    { CURVEFS_ERROR::NO_FLUSH, { -1, "no flush" } },
    { CURVEFS_ERROR::NOT_SUPPORT, { EOPNOTSUPP, "operation not supported" } },
    { CURVEFS_ERROR::NAME_TOO_LONG, { ENAMETOOLONG, "file name too long" } },
    { CURVEFS_ERROR::MOUNT_POINT_EXIST, { -1, "mount point already exist" } },
    { CURVEFS_ERROR::MOUNT_FAILED, { -1, "mount failed" } },
    { CURVEFS_ERROR::OUT_OF_RANGE, { ERANGE, "out of range" } },
    { CURVEFS_ERROR::NO_DATA, { ENODATA, "no data available" } },
    { CURVEFS_ERROR::IO_ERROR, { EIO, "I/O error" } },
    { CURVEFS_ERROR::STALE, { ESTALE, "stale file handler" } },
    { CURVEFS_ERROR::NOSYS, { ENOSYS, "invalid system call" } },
    { CURVEFS_ERROR::END_OF_FILE, { EOF, "end of file" } },
    { CURVEFS_ERROR::NOT_A_DIRECTORY, { ENOTDIR, "not a directory" } },
};

std::string StrErr(CURVEFS_ERROR code) {
    auto it = errors.find(code);
    if (it != errors.end()) {
        return it->second.second;
    }
    return "unknown";
}

int SysErr(CURVEFS_ERROR code) {
    int syscode = -1;
    auto it = errors.find(code);
    if (it != errors.end()) {
        syscode = it->second.first;
    }
    return (syscode == -1) ? EIO : syscode;
}

std::ostream &operator<<(std::ostream &os, CURVEFS_ERROR code) {
    os << static_cast<int>(code) << "[" << [code]() {
        auto it = errors.find(code);
        if (it != errors.end()) {
            return it->second.second;
        }

        return std::string{"Unknown"};
    }() << "]";

    return os;
}

CURVEFS_ERROR ToFSError(MetaStatusCode code) {
    static std::map<MetaStatusCode, CURVEFS_ERROR> errs = {
        { MetaStatusCode::OK, CURVEFS_ERROR::OK },
        { MetaStatusCode::NOT_FOUND, CURVEFS_ERROR::NOT_EXIST },
        { MetaStatusCode::PARAM_ERROR, CURVEFS_ERROR::INVALID_PARAM },
        { MetaStatusCode::INODE_EXIST, CURVEFS_ERROR::EXISTS },
        { MetaStatusCode::DENTRY_EXIST, CURVEFS_ERROR::EXISTS },
        { MetaStatusCode::SYM_LINK_EMPTY, CURVEFS_ERROR::INTERNAL },
        { MetaStatusCode::RPC_ERROR, CURVEFS_ERROR::INTERNAL },
    };

    auto it = errs.find(code);
    if (it != errs.end()) {
        return it->second;
    }
    return CURVEFS_ERROR::UNKNOWN;
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
