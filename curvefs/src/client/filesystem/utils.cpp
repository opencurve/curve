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

#include <glog/logging.h>

#include "curvefs/src/client/filesystem/utils.h"

namespace curvefs {
namespace client {
namespace filesystem {

bool IsDir(const InodeAttr& attr) {
    return attr.type() == FsFileType::TYPE_DIRECTORY;
}

bool IsS3File(const InodeAttr& attr) {
    return attr.type() == FsFileType::TYPE_S3;
}

bool IsVolmeFile(const InodeAttr& attr) {
    return attr.type() == FsFileType::TYPE_FILE;
}

bool IsSymLink(const InodeAttr& attr) {
    return attr.type() == FsFileType::TYPE_SYM_LINK;
}

TimeSpec AttrMtime(const InodeAttr& attr) {
    return TimeSpec(attr.mtime(), attr.mtime_ns());
}

TimeSpec AttrCtime(const InodeAttr& attr) {
    return TimeSpec(attr.ctime(), attr.ctime_ns());
}

TimeSpec InodeMtime(const std::shared_ptr<InodeWrapper> inode) {
    InodeAttr attr;
    inode->GetInodeAttr(&attr);
    return AttrMtime(attr);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
