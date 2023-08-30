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
 * Created Date: 2023-07-19
 * Author: Jingli Chen (Wine93)
 */

#include <cstdint>
#include <string>
#include <map>

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_XATTR_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_XATTR_H_

namespace curvefs {
namespace client {
namespace filesystem {

const uint32_t MAX_XATTR_NAME_LENGTH = 255;
const uint32_t MAX_XATTR_VALUE_LENGTH = 64 * 1024;

const char XATTR_DIR_FILES[] = "curve.dir.files";
const char XATTR_DIR_SUBDIRS[] = "curve.dir.subdirs";
const char XATTR_DIR_ENTRIES[] = "curve.dir.entries";
const char XATTR_DIR_FBYTES[] = "curve.dir.fbytes";
const char XATTR_DIR_RFILES[] = "curve.dir.rfiles";
const char XATTR_DIR_RSUBDIRS[] = "curve.dir.rsubdirs";
const char XATTR_DIR_RENTRIES[] = "curve.dir.rentries";
const char XATTR_DIR_RFBYTES[] = "curve.dir.rfbytes";
const char XATTR_DIR_PREFIX[] = "curve.dir";
const char XATTR_WARMUP_OP[] = "curvefs.warmup.op";
const char XATTR_ACL_ACCESS[] = "system.posix_acl_access";
const char XATTR_ACL_DEFAULT[] = "system.posix_acl_default";

inline bool IsSpecialXAttr(const std::string& key) {
    static std::map<std::string, bool> xattrs {
        { XATTR_DIR_FILES, true },
        { XATTR_DIR_SUBDIRS, true },
        { XATTR_DIR_ENTRIES, true },
        { XATTR_DIR_FBYTES, true },
        { XATTR_DIR_RFILES, true },
        { XATTR_DIR_RSUBDIRS, true },
        { XATTR_DIR_RENTRIES, true },
        { XATTR_DIR_RFBYTES, true },
        { XATTR_DIR_PREFIX, true },
        { XATTR_ACL_ACCESS, true },
        { XATTR_ACL_DEFAULT, true }
    };
    return xattrs.find(key) != xattrs.end();
}

inline bool IsWarmupXAttr(const std::string& key) {
    return key == XATTR_WARMUP_OP;
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_XATTR_H_
