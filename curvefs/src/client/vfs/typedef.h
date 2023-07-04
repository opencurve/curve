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
 * Created Date: 2023-09-20
 * Author: Jingli Chen (Wine93)
 */

// This file define the alias type for all kinds of Fuse related type,
// so those types must be synchronized with Fuse if changed.

#ifndef CURVEFS_SRC_CLIENT_VFS_TYPEDEF_H_
#define CURVEFS_SRC_CLIENT_VFS_TYPEDEF_H_

#include <string>
#include <memory>

namespace curvefs {
namespace client {
namespace vfs {

enum AttrMask {
    SET_ATTR_MODE = FUSE_SET_ATTR_MODE,
    SET_ATTR_UID = FUSE_SET_ATTR_UID,
    SET_ATTR_GID = FUSE_SET_ATTR_GID,
};

struct FuseContext {
    struct mock_fuse_req {
        void* se;
        uint64_t unique;
        int ctr;
        pthread_mutex_t lock;
        struct fuse_ctx ctx;
    };

    FuseContext(uint32_t uid, uint32_t gid, uint32_t umask) {
        // ctx
        ctx = fuse_ctx();
        ctx.uid = uid;
        ctx.gid = gid;
        ctx.umask = umask;
        // req
        mock_req = mock_fuse_req();
        mock_req.ctx = ctx;
        // fi
        file_info = fuse_file_info();
    }

    fuse_req_t GetRequest() {
        return reinterpret_cast<fuse_req_t>(&mock_req);
    }

    fuse_file_info* GetFileInfo() {
        return &file_info;
    }

    fuse_ctx ctx;
    mock_fuse_req mock_req;
    fuse_file_info file_info;
};

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VFS_TYPEDEF_H_
