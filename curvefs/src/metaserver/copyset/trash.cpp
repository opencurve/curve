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
 * Created Date: Sun 29 Aug 2021 03:29:15 PM CST
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/trash.h"

#include <fcntl.h>

#include <string>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_cat.h"
#include "curvefs/src/metaserver/copyset/utils.h"
#include "src/common/string_util.h"
#include "src/common/uri_parser.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curve::common::StringToUll;
using ::curve::common::UriParser;
using ::curve::fs::LocalFileSystem;

CopysetTrash::CopysetTrash()
    : options_(),
      lfs_(nullptr),
      trashDir_(),
      running_(false),
      recycleThread_(),
      sleeper_() {}

bool CopysetTrash::Init(const CopysetTrashOptions& options,
                        LocalFileSystem* fs) {
    options_ = options;
    lfs_ = fs;

    bool parseSuccess =
        !UriParser::ParseUri(options_.trashUri, &trashDir_).empty() &&
        !trashDir_.empty();
    if (!parseSuccess) {
        LOG(ERROR) << "Trash uri is invalid, trash uri: " << options_.trashUri;
        return false;
    }

    LOG(INFO) << "Trash init success, trash path: '" << trashDir_ << "'";
    return true;
}

bool CopysetTrash::Start() {
    if (!running_.exchange(true)) {
        recycleThread_ =
            std::thread(&CopysetTrash::DeleteExpiredCopysets, this);
        LOG(INFO) << "Trash thread start success";
    }

    return true;
}

bool CopysetTrash::Stop() {
    if (running_.exchange(false)) {
        LOG(INFO) << "Stop trash...";
        sleeper_.interrupt();
        recycleThread_.join();
        LOG(INFO) << "Trash stopped";
    }

    return true;
}

std::string CopysetTrash::GenerateCopysetRecyclePath(
    const std::string& copysetAbsolutePath) {
    std::string copysetBasename =
        copysetAbsolutePath.substr(copysetAbsolutePath.find_last_of('/') + 1);

    std::string destPath =
        trashDir_ + "/" + copysetBasename + "." + std::to_string(time(nullptr));
    return destPath;
}

bool CopysetTrash::IsCopysetDirNameValid(const std::string& dir) const {
    const auto pos = dir.find('.');
    if (pos == std::string::npos) {
        LOG(WARNING) << "'" << dir << "' is invalid";
        return false;
    }

    uint64_t groupId;
    bool success = StringToUll(dir.substr(0, pos), &groupId);
    if (!success) {
        LOG(WARNING) << "Convert to group id failed, group id string: "
                     << dir.substr(0, pos);
        return false;
    }

    if (GetPoolId(groupId) <= 0 || GetCopysetId(groupId) <= 0) {
        LOG(WARNING) << "Invalid copyset group id: " << groupId;
        return false;
    }

    return true;
}

bool CopysetTrash::IsCopysetDirExpired(const std::string& dir) {
    int fd = lfs_->Open(dir, O_RDONLY);
    if (fd < 0) {
        LOG(ERROR) << "Trash open dir failed, dir: " << dir;
        return false;
    }

    auto closeFd = absl::MakeCleanup([fd, this]() { lfs_->Close(fd); });

    struct stat dirInfo;
    if (0 != lfs_->Fstat(fd, &dirInfo)) {
        LOG(ERROR) << "Trash stat dir failed, dir: " << dir;
        return false;
    }

    time_t now = time(nullptr);
    if (difftime(now, dirInfo.st_ctime) < options_.expiredAfterSec) {
        return false;
    }

    return true;
}

bool CopysetTrash::RecycleCopyset(const std::string& copysetAbsolutePath) {
    if (!CreateTrashDirIfNotExist()) {
        LOG(WARNING) << "Create trash path failed";
        return false;
    }

    std::string destPath = GenerateCopysetRecyclePath(copysetAbsolutePath);
    if (lfs_->DirExists(destPath)) {
        LOG(WARNING) << "Recycle copyset failed, dest path already exists, "
                     << "copyset dir: " << copysetAbsolutePath
                     << ", dest dir: " << destPath;
        return false;
    }

    if (0 != lfs_->Rename(copysetAbsolutePath, destPath)) {
        LOG(ERROR) << "Recycle copyset rename failed, copyset dir: "
                   << copysetAbsolutePath << ", dest dir: " << destPath;
        return false;
    }

    LOG(INFO) << "Recycle copyset success, copyset dir: " << copysetAbsolutePath
              << ", dest dir: " << destPath;
    return true;
}

void CopysetTrash::DeleteExpiredCopysets() {
    while (sleeper_.wait_for(std::chrono::seconds(options_.scanPeriodSec))) {
        if (!lfs_->DirExists(trashDir_)) {
            continue;
        }

        std::vector<std::string> subdirs;
        if (0 != lfs_->List(trashDir_, &subdirs)) {
            LOG(ERROR) << "Trash list '" << trashDir_ << "' failed";
            continue;
        }

        for (const auto& subdir : subdirs) {
            if (!IsCopysetDirNameValid(subdir)) {
                continue;
            }

            std::string fullpath = absl::StrCat(trashDir_, "/", subdir);
            if (!IsCopysetDirExpired(fullpath)) {
                continue;
            }

            if (0 != lfs_->Delete(fullpath)) {
                LOG(ERROR) << "Trash delete dir failed, " << fullpath;
            } else {
                LOG(INFO) << "Trash delete dir succeeded, " << fullpath;
            }
        }
    }
}

bool CopysetTrash::CreateTrashDirIfNotExist() {
    if (!lfs_->DirExists(trashDir_)) {
        LOG(INFO) << "Trash dir not exist, going to create it";

        if (0 != lfs_->Mkdir(trashDir_)) {
            LOG(ERROR) << "Trash dir create failed, trash path: " << trashDir_;
            return false;
        }
    }

    return true;
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
