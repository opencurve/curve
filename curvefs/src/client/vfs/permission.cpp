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
 * Created Date: 2023-07-07
 * Author: Jingli Chen (Wine93)
 */

#include <algorithm>

#include "curvefs/src/client/vfs/meta.h"
#include "curvefs/src/client/vfs/permission.h"

namespace curvefs {
namespace client {
namespace vfs {

Permission::Permission(UserPermissionOption userPerm)
    : userPerm_(userPerm) {}

uint16_t Permission::GetFileMode(uint16_t type, uint16_t mode) {
    uint16_t umask = userPerm_.umask;
    mode = mode & (~umask);
    return type | mode;  // e.g. S_IFREG | mode
}

uint16_t Permission::WantPermission(uint32_t flags) {
    uint16_t want = 0;
    switch (flags & O_ACCMODE) {
        case O_RDONLY:
            want = WANT_READ;
            break;

        case O_WRONLY:
            want = WANT_WRITE;
            break;

        case O_RDWR:
            want = WANT_READ | WANT_WRITE;
            break;
    }

    if (flags & O_TRUNC) {
        want |= WANT_WRITE;
    }
    return want;
}

bool Permission::IsSuperUser() {
    return userPerm_.uid == 0;
}

bool Permission::IsFileOwner(const InodeAttr& file) {
    return userPerm_.uid == file.uid();
}

bool Permission::GidInGroup(uint16_t gid) {
    auto gids = userPerm_.gids;
    return std::find(gids.begin(), gids.end(), gid) != gids.end();
}

uint16_t Permission::GetFilePermission(const InodeAttr& file) {
    uint16_t mode = file.mode();
    if (userPerm_.uid == file.uid()) {
        mode = mode >> 6;
    } else if (GidInGroup(file.gid())) {
        mode = mode >> 3;
    }
    return mode & 7;
}

CURVEFS_ERROR Permission::Check(const InodeAttr& file, uint16_t want) {
    if (IsSuperUser()) {
        return CURVEFS_ERROR::OK;
    }

    auto perm = GetFilePermission(file);
    if ((perm & want) != want) {
        return CURVEFS_ERROR::NO_PERMISSION;
    }
    return CURVEFS_ERROR::OK;
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

